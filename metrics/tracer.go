package metrics

import (
	"context"
	"log"
	"time"
)

type Tracer interface {
	StartSpan(ctx context.Context, name string) (context.Context, Span)
}

type Span interface {
	End()
	SetAttributes(attrs map[string]any)
}

type NoopTracer struct{}

func (nt *NoopTracer) StartSpan(ctx context.Context, _ string) (context.Context, Span) {
	return ctx, &NoopSpan{}
}

type NoopSpan struct{}

func (ns *NoopSpan) End()                           {}
func (ns *NoopSpan) SetAttributes(_ map[string]any) {}

type Collector interface {
	IncCommitCount()
	IncRollbackCount()
	RecordCommitDuration(d time.Duration)
	RecordRollbackDuration(d time.Duration)
	IncErrorCount()
	IncRetryCount()
	RecordActionDuration(action string, d time.Duration)
}

type NoopMetricsCollector struct{}

func (n *NoopMetricsCollector) IncCommitCount()                                {}
func (n *NoopMetricsCollector) IncRollbackCount()                              {}
func (n *NoopMetricsCollector) RecordCommitDuration(_ time.Duration)           {}
func (n *NoopMetricsCollector) RecordRollbackDuration(_ time.Duration)         {}
func (n *NoopMetricsCollector) IncErrorCount()                                 {}
func (n *NoopMetricsCollector) IncRetryCount()                                 {}
func (n *NoopMetricsCollector) RecordActionDuration(_ string, _ time.Duration) {}

type PrometheusMetricsCollector struct{}

func (p *PrometheusMetricsCollector) IncCommitCount() {
	log.Println("Prometheus: commit count incremented")
}
func (p *PrometheusMetricsCollector) IncRollbackCount() {
	log.Println("Prometheus: rollback count incremented")
}
func (p *PrometheusMetricsCollector) RecordCommitDuration(d time.Duration) {
	log.Printf("Prometheus: commit duration %v", d)
}
func (p *PrometheusMetricsCollector) RecordRollbackDuration(d time.Duration) {
	log.Printf("Prometheus: rollback duration %v", d)
}
func (p *PrometheusMetricsCollector) IncErrorCount() {
	log.Println("Prometheus: error count incremented")
}
func (p *PrometheusMetricsCollector) IncRetryCount() {
	log.Println("Prometheus: retry count incremented")
}
func (p *PrometheusMetricsCollector) RecordActionDuration(action string, d time.Duration) {
	log.Printf("Prometheus: action %s duration %v", action, d)
}
