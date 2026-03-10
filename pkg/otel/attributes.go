package otel

import (
	"github.com/obot-platform/kinm/pkg/types"

	"go.opentelemetry.io/otel/attribute"
	"k8s.io/apiserver/pkg/storage"
)

func ListOptionsToAttributes(opts storage.ListOptions, otherAttributes ...attribute.KeyValue) []attribute.KeyValue {
	result := append([]attribute.KeyValue{}, otherAttributes...)
	result = append(result,
		attribute.Bool("hasResourceVersion", opts.ResourceVersion != ""),
		attribute.Bool("hasContinue", opts.Predicate.Continue != ""),
		attribute.Int64("limit", opts.Predicate.Limit),
		attribute.Bool("hasLabelSelector", hasSelector(opts.Predicate.Label)),
		attribute.Bool("hasFieldSelector", hasSelector(opts.Predicate.Field)),
		attribute.Bool("allowWatchBookmarks", opts.Predicate.AllowWatchBookmarks),
		attribute.Int("indexLabelCount", len(opts.Predicate.IndexLabels)),
		attribute.Int("indexFieldCount", len(opts.Predicate.IndexFields)),
		attribute.Bool("hasResourceVersionMatch", opts.ResourceVersionMatch != ""),
		attribute.Bool("progressNotify", opts.ProgressNotify),
		attribute.Bool("recursive", opts.Recursive),
	)
	if opts.SendInitialEvents != nil {
		result = append(result, attribute.Bool("sendInitialEvents", *opts.SendInitialEvents))
	}
	return result
}

func ObjectToAttributes(obj types.Object, otherAttributes ...attribute.KeyValue) []attribute.KeyValue {
	result := append([]attribute.KeyValue{}, otherAttributes...)
	if namespace := obj.GetNamespace(); namespace != "" {
		result = append(result, attribute.String("namespace", namespace))
	}
	return result
}

type stringer interface {
	String() string
}

func hasSelector(selector stringer) bool {
	return selector != nil && selector.String() != ""
}
