package balancer

import (
	"context"
	"fmt"
	"net"
	"strings"
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
	type testCaseData struct {
		info                  string
		endpointsPerDC        map[string]int
		localDC               string
		totalRequests         int
		localDCMinLoadPercent float64
		localDCMaxLoadPercent float64
	}

	testCases := []testCaseData{
		{
			info: "100_100_2",
			endpointsPerDC: map[string]int{
				"dc1": 100,
				"dc2": 100,
				"dc3": 2,
			},
			localDC:               "dc3",
			totalRequests:         1000,
			localDCMinLoadPercent: 30,
			localDCMaxLoadPercent: 80,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			cfg := config.New(
				config.WithBalancer(
					balancers.PreferNearestDC(
						balancers.RandomChoice(),
					),
				),
			)

			pool := conn.NewPool(context.Background(), cfg)

			var endpoints []endpoint.Endpoint
			endpointCounter := make(map[string]int)

			for dc, amountEndpoints := range tc.endpointsPerDC {
				for i := 0; i < amountEndpoints; i++ {
					endpointCounter[dc]++
					endpoints = append(endpoints, &mock.Endpoint{
						AddrField:     fmt.Sprintf("grpc://%s-%d:123", dc, endpointCounter[dc]),
						LocationField: dc,
					})
				}
			}

			r := &Balancer{
				driverConfig:   cfg,
				balancerConfig: *cfg.Balancer(),
				pool:           pool,
				discover: func(ctx context.Context, _ *grpc.ClientConn) ([]endpoint.Endpoint, string, error) {
					return endpoints, "", nil
				},
				localDCDetector: func(ctx context.Context, eps []endpoint.Endpoint) (string, error) {
					return tc.localDC, nil
				},
			}

			err := r.clusterDiscoveryAttempt(context.Background(), nil)
			require.NoError(t, err)

			dcCounter := make(map[string]int)

			for i := 0; i < tc.totalRequests; i++ {
				c, _ := r.connections().GetConnection(context.Background())
				loc := c.Endpoint().Location()
				dcCounter[loc]++
			}

			distributionBuilder := strings.Builder{}
			distributionBuilder.WriteString("Distribution per DC:")
			for dc := range tc.endpointsPerDC {
				distributionBuilder.WriteString(fmt.Sprintf("\n--- %s: %d (%.1f%%)", dc, dcCounter[dc],
					float64(dcCounter[dc])/float64(tc.totalRequests)*100))
			}

			t.Log(distributionBuilder.String())

			localDCPercentage := float64(dcCounter[tc.localDC]) / float64(tc.totalRequests) * 100

			require.Greater(t, localDCPercentage, tc.localDCMinLoadPercent,
				"Local DC should handle at least %.1f%% (was %.1f%%)", tc.localDCMinLoadPercent, localDCPercentage,
			)

			require.Less(t, localDCPercentage, tc.localDCMinLoadPercent,
				"Local DC should handle at most %.1f%% (was %.1f%%)", tc.localDCMaxLoadPercent, localDCPercentage,
			)
		})
	}
}

func TestBalancer_TestSmallDCRequestsHandlingSimulation(t *testing.T) {
	type testCaseData struct {
		info                  string
		endpointsPerDC        map[string]int
		localDC               string
		maxConnsPerEndpoint   int
		workDuration          time.Duration
		totalRequests         int
		localDCMinLoadPercent float64
		localDCMaxLoadPercent float64
	}

	testCases := []testCaseData{
		{
			info: "100_100_2",
			endpointsPerDC: map[string]int{
				"dc1": 100,
				"dc2": 100,
				"dc3": 2,
			},
			localDC:               "dc3",
			maxConnsPerEndpoint:   10,
			workDuration:          10 * time.Millisecond,
			totalRequests:         1000,
			localDCMinLoadPercent: 30,
			localDCMaxLoadPercent: 80,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			cfg := config.New(
				config.WithBalancer(
					balancers.PreferNearestDC(
						balancers.RandomChoice(),
					),
				),
			)

			pool := conn.NewPool(context.Background(), cfg)

			type server struct {
				location  string
				semaphore chan struct{}
				success   atomic.Int32
				errors    atomic.Int32
			}

			servers := make(map[string]*server)
			var endpoints []endpoint.Endpoint
			endpointCounter := make(map[string]int)

			for dc, amountEndpoints := range tc.endpointsPerDC {
				for i := 0; i < amountEndpoints; i++ {
					endpointCounter[dc]++
					addr := fmt.Sprintf("grpc://%s-%d:123", dc, endpointCounter[dc])
					s := &server{
						location:  dc,
						semaphore: make(chan struct{}, tc.maxConnsPerEndpoint),
					}
					servers[addr] = s
					endpoints = append(endpoints, &mock.Endpoint{
						AddrField:     addr,
						LocationField: dc,
					})
				}
			}

			r := &Balancer{
				driverConfig:   cfg,
				balancerConfig: *cfg.Balancer(),
				pool:           pool,
				discover: func(ctx context.Context, _ *grpc.ClientConn) ([]endpoint.Endpoint, string, error) {
					return endpoints, "", nil
				},
				localDCDetector: func(ctx context.Context, eps []endpoint.Endpoint) (string, error) {
					return tc.localDC, nil
				},
			}

			err := r.clusterDiscoveryAttempt(context.Background(), nil)
			require.NoError(t, err)

			var wg sync.WaitGroup
			start := time.Now()
			successCount := atomic.Int32{}
			errorCount := atomic.Int32{}

			for i := 0; i < tc.totalRequests; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer cancel()

					conn, _ := r.connections().GetConnection(ctx)
					address := conn.Endpoint().Address()
					server, ok := servers[address]
					if !ok {
						errorCount.Add(1)
						t.Logf("Something wrong with server address: %s", address)
						return
					}

					select {
					case server.semaphore <- struct{}{}:
						defer func() { <-server.semaphore }()
						time.Sleep(tc.workDuration)
						server.success.Add(1)
						successCount.Add(1)
					default:
						server.errors.Add(1)
						errorCount.Add(1)
					}
				}()
			}

			wg.Wait()
			duration := time.Since(start)

			results := make(map[string]struct {
				success int32
				errors  int32
			})

			for _, srv := range servers {
				dcResult, ok := results[srv.location]
				if !ok {
					dcResult = struct {
						success int32
						errors  int32
					}{0, 0}
				}

				dcResult.success += srv.success.Load()
				dcResult.errors += srv.errors.Load()
				results[srv.location] = dcResult
			}

			totalSuccess := successCount.Load()
			totalErrors := errorCount.Load()
			localDCLoad := results[tc.localDC]
			localDCTotal := localDCLoad.success + localDCLoad.errors
			localDCPercentage := float64(localDCTotal) / float64(tc.totalRequests) * 100

			distributionBuilder := strings.Builder{}
			distributionBuilder.WriteString("--- Distribution per DC:")
			for dc := range tc.endpointsPerDC {
				resultDC := results[dc]
				allRequestsToDC := resultDC.success + resultDC.errors
				percentage := float64(allRequestsToDC) / float64(tc.totalRequests) * 100
				distributionBuilder.WriteString(fmt.Sprintf("\n--- %s: %d (%.1f%%) [success: %d, errors: %d]",
					dc, allRequestsToDC, percentage, resultDC.success, resultDC.errors))
			}

			t.Logf("Test case %s", tc.info)
			t.Logf("Configuration:")
			t.Logf("--- Endpoints per DC: %v", tc.endpointsPerDC)
			t.Logf("--- Local DC: %s", tc.localDC)
			t.Logf("--- Max conns per endpoint: %d", tc.maxConnsPerEndpoint)
			t.Logf("--- Work duration: %v", tc.workDuration)
			t.Logf("--- Total requests: %d", tc.totalRequests)
			t.Logf("Results:")
			t.Logf("--- Successful requests: %d (%.1f%%)", totalSuccess, float64(totalSuccess)/float64(tc.totalRequests)*100)
			t.Logf("--- Failed requests: %d (%.1f%%)", totalErrors, float64(totalErrors)/float64(tc.totalRequests)*100)
			t.Log(distributionBuilder.String())
			t.Logf("--- Local DC (%s) load: %d requests (%.1f%%)", tc.localDC, localDCTotal, localDCPercentage)
			t.Logf("--- Total duration: %v", duration)

			require.Greater(t, localDCPercentage, tc.localDCMinLoadPercent,
				"Local DC should handle at least %.1f%% (was %.1f%%)", tc.localDCMinLoadPercent, localDCPercentage,
			)

			require.Less(t, localDCPercentage, tc.localDCMinLoadPercent,
				"Local DC should handle at most %.1f%% (was %.1f%%)", tc.localDCMaxLoadPercent, localDCPercentage,
			)

			require.Greater(t,
				totalSuccess, int32(tc.totalRequests*9/10),
				"Success rate should be at least 90%% (was %.1f%%)",
				float64(totalSuccess)/float64(tc.totalRequests)*100,
			)
		})
	}
}
