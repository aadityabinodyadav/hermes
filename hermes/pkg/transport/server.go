package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/aadityabinodyadav/hermes/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	config Config

	grpcServer *grpc.Server
	listener   net.Listener

	raftHandler       pb.RaftServiceServer
	membershipHandler pb.MembershipServiceServer
	kvHandler         pb.HermesKVServer

	mu sync.RWMutex

	started bool
	stopped bool

	metrics *MetricsCollector
}

func NewServer(config Config) (*Server, error) {
	metrics := NewMetricsCollector()

	chainedInterceptor := ChainUnaryInterceptors(
		LoggingInterceptor,
		RecoveryInterceptor,
		TimeoutInterceptor(config.RequestTimeout),
		metrics.UnaryInterceptor(),
	)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(chainedInterceptor),

		grpc.MaxRecvMsgSize(config.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(config.MaxSendMsgSize),

		grpc.MaxConcurrentStreams(config.MaxConcurrentStreams),

		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Second,
			Time:              config.KeepAliveTime,
			Timeout:           config.KeepAliveTimeout,
		}),

		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),

		grpc.WriteBufferSize(config.WriteBufferSize),
		grpc.ReadBufferSize(config.ReadBufferSize),
	)

	server := &Server{
		config:     config,
		grpcServer: grpcServer,
		metrics:    metrics,
	}

	reflection.Register(grpcServer)

	return server, nil
}

func (s *Server) RegisterRaftService(handler pb.RaftServiceServer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.raftHandler = handler

	pb.RegisterRaftServiceServer(
		s.grpcServer,
		handler,
	)
}

func (s *Server) RegisterMembershipService(
	handler pb.MembershipServiceServer,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.membershipHandler = handler

	pb.RegisterMembershipServiceServer(
		s.grpcServer,
		handler,
	)
}

func (s *Server) RegisterKVService(handler pb.HermesKVServer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.kvHandler = handler

	pb.RegisterHermesKVServer(
		s.grpcServer,
		handler,
	)
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("server already started")
	}

	lis, err := net.Listen(
		"tcp",
		s.config.ListenAddr,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to listen on %s: %w",
			s.config.ListenAddr,
			err,
		)
	}

	s.listener = lis
	s.started = true

	go func() {
		fmt.Printf(
			"Hermes node %s listening on %s\n",
			s.config.NodeID,
			s.config.ListenAddr,
		)

		if err := s.grpcServer.Serve(lis); err != nil {
			s.mu.RLock()
			stopped := s.stopped
			s.mu.RUnlock()

			if !stopped {
				fmt.Printf(
					"gRPC server error: %v\n",
					err,
				)
			}
		}
	}()

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	stopped := make(chan struct{})

	go func() {
		s.grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		fmt.Printf(
			"Server %s stopped gracefully\n",
			s.config.NodeID,
		)
		return nil

	case <-ctx.Done():
		s.grpcServer.Stop()

		fmt.Printf(
			"Server %s force stopped (context deadline exceeded)\n",
			s.config.NodeID,
		)

		return ctx.Err()
	}
}

func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.listener != nil {
		return s.listener.Addr().String()
	}

	return s.config.ListenAddr
}
