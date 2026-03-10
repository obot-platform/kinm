package otel

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func StartSpanIfParent(ctx context.Context, tracer trace.Tracer, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return StartSpanLevelIfParent(ctx, tracer, LevelBasic, name, opts...)
}

func StartSpanLevelIfParent(ctx context.Context, tracer trace.Tracer, min Level, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !CurrentLevel().Enabled(min) {
		return ctx, noop.Span{}
	}
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, noop.Span{}
	}
	if tracer == nil {
		return ctx, noop.Span{}
	}
	return tracer.Start(ctx, name, opts...)
}
