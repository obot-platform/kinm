package otel

import (
	"github.com/obot-platform/kinm/pkg/types"

	"go.opentelemetry.io/otel/attribute"
	"k8s.io/apiserver/pkg/storage"
)

func ListOptionsToAttributes(opts storage.ListOptions, otherAttributes ...attribute.KeyValue) []attribute.KeyValue {
	return append(otherAttributes,
		attribute.String("resourceVersion", opts.ResourceVersion),
		attribute.String("continue", opts.Predicate.Continue),
		attribute.Int64("limit", opts.Predicate.Limit),
		attribute.Bool("allowWatchBookmarks", opts.Predicate.AllowWatchBookmarks),
		attribute.StringSlice("indexLabels", opts.Predicate.IndexLabels),
		attribute.StringSlice("indexFields", opts.Predicate.IndexFields),
		attribute.Stringer("labelSelector", opts.Predicate.Label),
		attribute.Stringer("fieldSelector", opts.Predicate.Field),
		attribute.String("resourceVersionMatch", string(opts.ResourceVersionMatch)),
		attribute.Bool("progressNotify", opts.ProgressNotify),
		attribute.Bool("recursive", opts.Recursive),
		attribute.Bool("sendInitialEvents", opts.SendInitialEvents == nil || *opts.SendInitialEvents),
	)
}

func ObjectToAttributes(obj types.Object, otherAttributes ...attribute.KeyValue) []attribute.KeyValue {
	return append(otherAttributes,
		attribute.String("name", obj.GetName()),
		attribute.String("namespace", obj.GetNamespace()),
	)
}
