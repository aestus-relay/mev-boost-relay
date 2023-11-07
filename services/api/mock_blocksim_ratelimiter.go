package api

import (
	"context"

	"github.com/flashbots/mev-boost-relay/common"
	"go.opentelemetry.io/otel"
)

type MockBlockSimulationRateLimiter struct {
	simulationError error
}

func (m *MockBlockSimulationRateLimiter) Send(context context.Context, payload *common.BuilderBlockValidationRequest, isHighPrio, fastTrack bool) (error, error) {
	context, span := otel.Tracer("api").Start(context, "MockBlockSimulationRateLimiter.Send")
	defer span.End()

	_ = context

	return nil, m.simulationError
}

func (m *MockBlockSimulationRateLimiter) CurrentCounter() int64 {
	return 0
}
