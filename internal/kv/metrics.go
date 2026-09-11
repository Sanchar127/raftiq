package kv

type KVMetrics interface {
	IncOperation(operation string)
	IncOperationError(operation string)
}

type NoopKVMetrics struct{}

func (NoopKVMetrics) IncOperation(string)      {}
func (NoopKVMetrics) IncOperationError(string) {}
