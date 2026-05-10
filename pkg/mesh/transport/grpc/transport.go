package grpc

import (
	"context"

	"github.com/jaksonkallio/monotonic/pkg/mesh/protobuf"
)

type Transport struct {
	protobuf.UnimplementedSynchronousRelayServiceServer
}

func (t *Transport) PollEvents(context.Context, *protobuf.PollEventsRequest) (*protobuf.EventStream, error) {
	return
}
