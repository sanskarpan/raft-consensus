// Package tracing provides OpenTelemetry initialization helpers for raftd.
package tracing

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const serviceName = "raftd"

// Provider wraps the OTel TracerProvider and handles shutdown.
type Provider struct {
	tp *sdktrace.TracerProvider
}

// NewNoopProvider returns a Provider that discards all traces.
// Used when OTLP endpoint is not configured.
func NewNoopProvider() *Provider {
	otel.SetTracerProvider(noop.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return &Provider{}
}

// NewOTLPProvider creates a TracerProvider that exports to an OTLP endpoint.
// endpoint example: "localhost:4317"
func NewOTLPProvider(ctx context.Context, endpoint, nodeID string) (*Provider, error) {
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceInstanceID(nodeID),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Provider{tp: tp}, nil
}

// Shutdown flushes and stops the tracer provider.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.tp == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.tp.Shutdown(ctx)
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// StartSpan is a convenience wrapper to start a span.
func StartSpan(ctx context.Context, tracerName, spanName string) (context.Context, trace.Span) {
	return Tracer(tracerName).Start(ctx, spanName)
}
