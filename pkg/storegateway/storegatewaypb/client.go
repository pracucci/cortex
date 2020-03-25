package storegatewaypb

import (
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

func NewStoreGatewayClientFactory(cfg grpcclient.Config) client.PoolFactory {
	return func(addr string) (client.PoolClient, error) {
		return DialStoreGatewayClient(cfg, addr)
	}
}

func DialStoreGatewayClient(cfg grpcclient.Config, addr string) (*closableStoreGatewayClient, error) {
	opts := []grpc.DialOption{grpc.WithInsecure()}
	opts = append(opts, cfg.DialOption(instrumentation())...)
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &closableStoreGatewayClient{
		StoreGatewayClient: NewStoreGatewayClient(conn),
		HealthClient:       grpc_health_v1.NewHealthClient(conn),
		conn:               conn,
	}, nil
}

type closableStoreGatewayClient struct {
	StoreGatewayClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (c *closableStoreGatewayClient) Close() error {
	return c.conn.Close()
}

// TODO this is a duplicate of pkg/ingester/client
func instrumentation() ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	return []grpc.UnaryClientInterceptor{
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
			// TODO cortex_middleware.PrometheusGRPCUnaryInstrumentation(ingesterClientRequestDuration),
		}, []grpc.StreamClientInterceptor{
			otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()),
			middleware.StreamClientUserHeaderInterceptor,
			// TODO cortex_middleware.PrometheusGRPCStreamInstrumentation(ingesterClientRequestDuration),
		}
}
