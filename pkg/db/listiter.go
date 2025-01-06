package db

import (
	"context"
	"fmt"
	"iter"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apiserver/pkg/storage"
)

func newLister(ctx context.Context, db *db, namespace string, opts storage.ListOptions, after bool) (string, iter.Seq2[record, error], error) {
	var (
		rev, cont int64
		err       error
	)

	if opts.ResourceVersion != "" {
		rev, err = strconv.ParseInt(opts.ResourceVersion, 10, 64)
		if err != nil {
			return "", nil, fmt.Errorf("invalid resource version %q, failed to parse: %w", opts.ResourceVersion, err)
		}
	}

	if opts.Predicate.Continue != "" {
		contRev, contAfter, ok := strings.Cut(opts.Predicate.Continue, ":")
		if !ok || contRev == "" || contAfter == "" {
			return "", nil, fmt.Errorf("invalid continue token %q", opts.Predicate.Continue)
		}
		rev, err = strconv.ParseInt(contRev, 10, 64)
		if err != nil {
			return "", nil, fmt.Errorf("invalid continue token %q, failed to parse revision: %w", opts.Predicate.Continue, err)
		}
		cont, err = strconv.ParseInt(contAfter, 10, 64)
		if err != nil {
			return "", nil, fmt.Errorf("invalid continue token %q, failed to parse: %w", opts.Predicate.Continue, err)
		}
	}

	listMeta, records, err := db.list(ctx, getNamespace(namespace), getName(opts), rev, after, cont, opts.Predicate.Limit, opts.Predicate.Field)
	if err != nil {
		return "", nil, err
	}

	rev = listMeta.ListID

	return strconv.FormatInt(listMeta.ListID, 10), func(yield func(record, error) bool) {
		for {
			for _, record := range records {
				if !yield(record, nil) {
					return
				}
			}

			if opts.Predicate.Limit == 0 {
				// no limits mean we would have fetched all records initially
				return
			}

			if len(records) <= int(opts.Predicate.Limit) {
				// When we fetch we always get one more record than the limit. So if we have more records than the limit
				// we know there could be more records to fetch. So if it's <= limit we know there's no more records to fetch.
				return
			}

			// Continue to paginate records
			_, records, err = db.list(ctx, getNamespace(namespace), getName(opts), rev, false, records[len(records)-1].id, opts.Predicate.Limit, opts.Predicate.Field)
			if err != nil {
				yield(record{}, err)
				return
			}
		}
	}, nil
}

func getNamespace(namespace string) *string {
	if namespace == "" {
		return nil
	}
	return &namespace
}

func getName(opts storage.ListOptions) *string {
	if opts.Predicate.Field == nil {
		return nil
	}

	for _, req := range opts.Predicate.Field.Requirements() {
		if req.Field == "metadata.name" && req.Operator == selection.Equals && req.Value != "" {
			return &req.Value
		}
	}

	return nil
}
