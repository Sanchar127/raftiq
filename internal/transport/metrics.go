package transport

import "time"

type RPCMetrics interface {
	IncRPCRequest(method string)
	IncRPCError(method string)
	ObserveRPCDuration(method string, duration time.Duration)
}

type NoopRPCMetrics struct{}

func (NoopRPCMetrics) IncRPCRequest(string) {}

func (NoopRPCMetrics) IncRPCError(string) {}

func (NoopRPCMetrics) ObserveRPCDuration(string, time.Duration) {}
