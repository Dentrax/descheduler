package tracing

import (
	"context"
	"crypto/x509"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"
	"k8s.io/klog/v2"
)

func NewTraceConfig(endpoint, caCert, name, namespace string) (provider trace.TracerProvider, err error) {
	if endpoint != "" {
		ctx := context.Background()

		var opts []otlptracegrpc.Option

		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))

		if caCert != "" {
			data, err := os.ReadFile(caCert)
			if err != nil {
				klog.ErrorS(err, "failed to extract ca certificate required to generate the transport credentials")
				return nil, err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(data) {
				klog.Error("failed to create cert pool using the ca certificate provided for generating teh transport credentials")
				return nil, err
			}
			opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(pool, "")))
		} else {
			klog.Info("Enabling trace GRPC client in insecure mode")
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		client := otlptracegrpc.NewClient(opts...)

		exporter, err := otlptrace.New(ctx, client)
		if err != nil {
			klog.ErrorS(err, "failed to create an instance of the trace exporter")
			return nil, err
		}
		if name == "" {
			name = "descheduler_traces"
		}
		resourceOpts := []sdkresource.Option{sdkresource.WithAttributes(semconv.ServiceNameKey.String(name)), sdkresource.WithSchemaURL(semconv.SchemaURL)}
		if namespace != "" {
			resourceOpts = append(resourceOpts, sdkresource.WithAttributes(semconv.ServiceNamespaceKey.String(namespace)))
		}
		resource, err := sdkresource.New(ctx, resourceOpts...)
		if err != nil {
			klog.ErrorS(err, "failed to create traceable resource")
			return nil, err
		}

		spanProcessor := sdktrace.NewBatchSpanProcessor(exporter)
		provider = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithSpanProcessor(spanProcessor),
			sdktrace.WithResource(resource),
		)
	} else {
		klog.Info("Did not find a trace collector endpoint defined. Switching to NoopTraceProvider")
		provider = trace.NewNoopTracerProvider()
	}

	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(provider)
	klog.Info("Successfully setup trace provider")
	return
}

func StartSpan(ctx context.Context, tracerName string, traceOpts []trace.TracerOption, operationName string, attributes []attribute.KeyValue) (context.Context, trace.Span, func()) {
	spanCtx, span := otel.Tracer(tracerName, traceOpts...).Start(ctx, operationName, trace.WithAttributes(attributes...))
	return spanCtx, span, func() {
		span.End()
	}
}

func ShutdownTraceProvider(ctx context.Context, tp *sdktrace.TracerProvider) {
	if err := tp.Shutdown(ctx); err != nil {
		klog.ErrorS(err, "ran into an error trying to shutdown the trace provider")
		otel.Handle(err)
	}
}
