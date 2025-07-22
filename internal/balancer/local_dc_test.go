package balancer

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

var localIP = net.IPv4(127, 0, 0, 1)

type discoveryMock struct {
	endpoints []endpoint.Endpoint
}

// implement discovery.Client
func (d discoveryMock) Close(ctx context.Context) error {
	return nil
}

func (d discoveryMock) Discover(ctx context.Context) ([]endpoint.Endpoint, error) {
	return d.endpoints, nil
}

func TestCheckFastestAddress(t *testing.T) {
	ctx := context.Background()

	t.Run("Ok", func(t *testing.T) {
		var firstCount int64
		var secondCount int64

		for i := 0; i < 100; i++ {
			listen1, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
			require.NoError(t, err)
			listen2, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
			require.NoError(t, err)
			addr1 := listen1.Addr().String()
			addr2 := listen2.Addr().String()

			fastest := checkFastestAddress(ctx, []string{addr1, addr2})
			require.NotEmpty(t, fastest)

			switch fastest {
			case addr1:
				firstCount++
			case addr2:
				secondCount++
			default:
				require.Contains(t, []string{addr1, addr2}, fastest)
			}

			_ = listen1.Close()
			_ = listen2.Close()
		}
		require.NotEmpty(t, firstCount)
		require.NotEmpty(t, secondCount)
	})
	t.Run("HasError", func(t *testing.T) {
		listen1, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		listen2, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		addr1 := listen1.Addr().String()
		addr2 := listen2.Addr().String()

		_ = listen2.Close() // for can't accept connections

		fastest := checkFastestAddress(ctx, []string{addr1, addr2})
		require.Equal(t, addr1, fastest)

		_ = listen1.Close()
	})
	t.Run("AllErrors", func(t *testing.T) {
		listen1, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		listen2, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		addr1 := listen1.Addr().String()
		addr2 := listen2.Addr().String()

		_ = listen1.Close() // for can't accept connections
		_ = listen2.Close() // for can't accept connections

		res := checkFastestAddress(ctx, []string{addr1, addr2})
		require.Empty(t, res)
	})
}

func TestDetectLocalDC(t *testing.T) {
	ctx := context.Background()
	xtest.TestManyTimesWithName(t, "Ok", func(t testing.TB) {
		listen1, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		defer func() { _ = listen1.Close() }()

		listen2, err := net.ListenTCP("tcp", &net.TCPAddr{IP: localIP})
		require.NoError(t, err)
		listen2Addr := listen2.Addr().String()
		_ = listen2.Close() // force close, for not accept tcp connections

		dc, err := detectLocalDC(ctx, []endpoint.Endpoint{
			&mock.Endpoint{LocationField: "a", AddrField: "grpc://" + listen1.Addr().String()},
			&mock.Endpoint{LocationField: "b", AddrField: "grpc://" + listen2Addr},
		})
		require.NoError(t, err)
		require.Equal(t, "a", dc)
	})
	t.Run("Empty", func(t *testing.T) {
		res, err := detectLocalDC(ctx, nil)
		require.Equal(t, "", res)
		require.Error(t, err)
	})
	t.Run("OneDC", func(t *testing.T) {
		res, err := detectLocalDC(ctx, []endpoint.Endpoint{
			&mock.Endpoint{LocationField: "a"},
			&mock.Endpoint{LocationField: "a"},
		})
		require.NoError(t, err)
		require.Equal(t, "a", res)
	})
}

func TestLocalDCDiscovery(t *testing.T) {
	ctx := context.Background()
	cfg := config.New(
		config.WithBalancer(balancers.PreferNearestDC(balancers.Default())),
	)
	r := &Balancer{
		driverConfig:   cfg,
		balancerConfig: *cfg.Balancer(),
		pool:           conn.NewPool(context.Background(), cfg),
		discover: func(ctx context.Context, _ *grpc.ClientConn) (endpoints []endpoint.Endpoint, location string, err error) {
			return []endpoint.Endpoint{
				&mock.Endpoint{AddrField: "a:123", LocationField: "a"},
				&mock.Endpoint{AddrField: "b:234", LocationField: "b"},
				&mock.Endpoint{AddrField: "c:456", LocationField: "c"},
			}, "", nil
		},
		localDCDetector: func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error) {
			return "b", nil
		},
	}

	err := r.clusterDiscoveryAttempt(ctx, nil)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		conn, _ := r.connections().GetConnection(ctx)
		require.Equal(t, "b:234", conn.Endpoint().Address())
		require.Equal(t, "b", conn.Endpoint().Location())
	}
}

func TestExtractHostPort(t *testing.T) {
	table := []struct {
		name    string
		address string
		host    string
		port    string
		err     bool
	}{
		{
			"HostPort",
			"asd:123",
			"asd",
			"123",
			false,
		},
		{
			"HostPortSchema",
			"grpc://asd:123",
			"asd",
			"123",
			false,
		},
		{
			"NoPort",
			"host",
			"",
			"",
			true,
		},
		{
			"Empty",
			"",
			"",
			"",
			true,
		},
	}
	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			host, port, err := extractHostPort(test.address)
			require.Equal(t, test.host, host)
			require.Equal(t, test.port, port)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetRandomEndpoints(t *testing.T) {
	source := []endpoint.Endpoint{
		&mock.Endpoint{AddrField: "a"},
		&mock.Endpoint{AddrField: "b"},
		&mock.Endpoint{AddrField: "c"},
	}

	t.Run("ReturnSource", func(t *testing.T) {
		res := getRandomEndpoints(source, 3)
		require.Equal(t, source, res)

		res = getRandomEndpoints(source, 4)
		require.Equal(t, source, res)
	})
	xtest.TestManyTimesWithName(t, "SelectRandom", func(t testing.TB) {
		res := getRandomEndpoints(source, 2)
		require.Len(t, res, 2)
		for _, ep := range res {
			require.Contains(t, source, ep)
		}
		require.NotEqual(t, res[0], res[1])
	})
}

func TestBalancer_SmallDCDistribution(t *testing.T) {
	xtest.TestManyTimesWithName(t, "SmallLocalDCDistribution", func(t testing.TB) {
		cfg := config.New(
			config.WithBalancer(
				balancers.PreferNearestDC(
					balancers.RandomChoice(),
				),
			),
		)

		pool := conn.NewPool(context.Background(), cfg)

		var endpoints []endpoint.Endpoint
		for i := 0; i < 100; i++ {
			endpoints = append(endpoints, &mock.Endpoint{
				AddrField:     fmt.Sprintf("grpc://dc1-%d:123", i),
				LocationField: "dc1",
			})
		}
		for i := 0; i < 100; i++ {
			endpoints = append(endpoints, &mock.Endpoint{
				AddrField:     fmt.Sprintf("grpc://dc2-%d:123", i),
				LocationField: "dc2",
			})
		}
		for i := 0; i < 2; i++ {
			endpoints = append(endpoints, &mock.Endpoint{
				AddrField:     fmt.Sprintf("grpc://dc3-%d:123", i),
				LocationField: "dc3",
			})
		}

		r := &Balancer{
			driverConfig:   cfg,
			balancerConfig: *cfg.Balancer(),
			pool:           pool,
			discover: func(ctx context.Context, _ *grpc.ClientConn) ([]endpoint.Endpoint, string, error) {
				return endpoints, "", nil
			},
			localDCDetector: func(ctx context.Context, eps []endpoint.Endpoint) (string, error) {
				return "dc3", nil
			},
		}

		err := r.clusterDiscoveryAttempt(context.Background(), nil)
		require.NoError(t, err)

		dcCount := make(map[string]int)
		totalRequests := 1000

		for i := 0; i < totalRequests; i++ {
			c, _ := r.connections().GetConnection(context.Background())
			loc := c.Endpoint().Location()
			dcCount[loc]++
		}

		t.Logf("Distribution: dc1=%d (%.1f), dc2=%d (%.1f), dc3=%d (%.1f)",
			dcCount["dc1"], float64(dcCount["dc1"])/float64(totalRequests),
			dcCount["dc2"], float64(dcCount["dc2"])/float64(totalRequests),
			dcCount["dc3"], float64(dcCount["dc3"])/float64(totalRequests),
		)

		require.Less(t, dcCount["dc3"], totalRequests/3)

		require.Greater(t, dcCount["dc1"], 0)
		require.Greater(t, dcCount["dc2"], 0)
		require.Greater(t, dcCount["dc3"], 0)
	})
}

func TestBalancer_TestSmallDCRequestsHandlingSimulation(t *testing.T) {
	xtest.TestManyTimesWithName(t, "SmallDCRequestsHandlingSimulation", func(t testing.TB) {
		cfg := config.New(
			config.WithBalancer(
				balancers.PreferNearestDC(
					balancers.RandomChoice(),
				),
			),
		)

		pool := conn.NewPool(context.Background(), cfg)

		type server struct {
			location string
			sem      chan struct{}
			success  atomic.Int32
			errors   atomic.Int32
		}

		servers := make(map[string]*server)
		var endpoints []endpoint.Endpoint

		for i := 0; i < 100; i++ {
			addr := fmt.Sprintf("grpc://dc1-%d:123", i)
			s := &server{
				location: "dc1",
				sem:      make(chan struct{}, 10),
			}
			servers[addr] = s
			endpoints = append(endpoints, &mock.Endpoint{
				AddrField:     addr,
				LocationField: "dc1",
			})
		}

		for i := 0; i < 100; i++ {
			addr := fmt.Sprintf("grpc://dc2-%d:123", i)
			s := &server{
				location: "dc2",
				sem:      make(chan struct{}, 10),
			}
			servers[addr] = s
			endpoints = append(endpoints, &mock.Endpoint{
				AddrField:     addr,
				LocationField: "dc2",
			})
		}

		for i := 0; i < 2; i++ {
			addr := fmt.Sprintf("grpc://dc3-%d:123", i)
			s := &server{
				location: "dc3",
				sem:      make(chan struct{}, 10),
			}
			servers[addr] = s
			endpoints = append(endpoints, &mock.Endpoint{
				AddrField:     addr,
				LocationField: "dc3",
			})
		}

		r := &Balancer{
			driverConfig:   cfg,
			balancerConfig: *cfg.Balancer(),
			pool:           pool,
			discover: func(ctx context.Context, _ *grpc.ClientConn) ([]endpoint.Endpoint, string, error) {
				return endpoints, "", nil
			},
			localDCDetector: func(ctx context.Context, eps []endpoint.Endpoint) (string, error) {
				return "dc3", nil
			},
		}

		err := r.clusterDiscoveryAttempt(context.Background(), nil)
		require.NoError(t, err)

		totalRequests := 1000
		var wg sync.WaitGroup
		start := time.Now()
		successCount := atomic.Int32{}
		errorCount := atomic.Int32{}

		for i := 0; i < totalRequests; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				conn, _ := r.connections().GetConnection(ctx)

				addr := conn.Endpoint().Address()
				srv, ok := servers[addr]
				if !ok {
					errorCount.Add(1)
					return
				}

				select {
				case srv.sem <- struct{}{}:
					defer func() { <-srv.sem }()
					time.Sleep(10 * time.Millisecond)
					srv.success.Add(1)
					successCount.Add(1)
				default:
					srv.errors.Add(1)
					errorCount.Add(1)
				}
			}()
		}

		wg.Wait()
		duration := time.Since(start)

		var (
			dc1Success int32
			dc1Errors  int32

			dc2Success int32
			dc2Errors  int32

			dc3Success int32
			dc3Errors  int32
		)

		for _, srv := range servers {
			success := srv.success.Load()
			errors := srv.errors.Load()

			switch srv.location {
			case "dc1":
				dc1Success += success
				dc1Errors += errors
			case "dc2":
				dc2Success += success
				dc2Errors += errors
			case "dc3":
				dc3Success += success
				dc3Errors += errors
			}
		}

		totalSuccess := successCount.Load()
		totalErrors := errorCount.Load()
		dc3Total := dc3Success + dc3Errors
		dc3Percentage := float64(dc3Total) / float64(totalRequests) * 100

		t.Logf("Load test results:")
		t.Logf("Total requests: %d", totalRequests)
		t.Logf("Successful requests: %d (%.1f%%)", totalSuccess, float64(totalSuccess)/float64(totalRequests)*100)
		t.Logf("Failed requests: %d (%.1f%%)", totalErrors, float64(totalErrors)/float64(totalRequests)*100)
		t.Logf("DC1: success=%d, errors=%d", dc1Success, dc1Errors)
		t.Logf("DC2: success=%d, errors=%d", dc2Success, dc2Errors)
		t.Logf("DC3: success=%d, errors=%d", dc3Success, dc3Errors)
		t.Logf("DC3 load percentage: %.1f%%", dc3Percentage)
		t.Logf("Total duration: %v", duration)
		t.Logf("RPS: %.1f", float64(totalRequests)/duration.Seconds())

		require.Less(t,
			90.0, dc3Percentage,
			"DC3 should handle more than 90%% of requests (handled %.1f%%)", dc3Percentage,
		)

		require.Greater(t,
			totalSuccess, int32(totalRequests*9/10),
			"Success rate should be at least 90%% (was %.1f%%)",
			float64(totalSuccess)/float64(totalRequests)*100)
	})
}
