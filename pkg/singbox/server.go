package singbox

import (
	"context"
	"net"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pep"
)

type ServerOptions struct {
	Profile           string
	ListenAddr        string
	Credentials       identity.ServerCredentials
	Enrollment        *identity.EnrollmentService
	ChunkSize         int
	MaxSessions       int
	EnableTCP         bool
	EnableQUIC        bool
	Transport         string
}

type RouterHandler interface {
	RouteConnection(ctx context.Context, conn net.Conn, destination string)
	RoutePacketConnection(ctx context.Context, conn net.PacketConn, destination string)
}

type Server struct {
	pepServer *pep.Server
}

func NewServer(opts ServerOptions, handler RouterHandler) (*Server, error) {
	pepCfg := pep.ServerConfig{
		ListenAddr:  opts.ListenAddr,
		Credentials: opts.Credentials,
		Enrollment:  opts.Enrollment,
		EnableTCP:   opts.EnableTCP,
		EnableQUIC:  opts.EnableQUIC,
		DestinationPolicy: &singboxDestinationPolicy{
			handler: handler,
		},
	}
	if opts.MaxSessions > 0 {
		pepCfg.MaxSessions = opts.MaxSessions
	}

	pepServer, err := pep.NewServer(pepCfg)
	if err != nil {
		return nil, err
	}

	return &Server{
		pepServer: pepServer,
	}, nil
}

func (s *Server) ServeListener(ctx context.Context, listener net.Listener) error {
	return s.pepServer.ServeListener(ctx, listener)
}

func (s *Server) ServePacketConn(ctx context.Context, conn net.PacketConn) error {
	return s.pepServer.ServePacketConn(ctx, conn)
}

type singboxDestinationPolicy struct {
	handler RouterHandler
}

func (p *singboxDestinationPolicy) DialContext(ctx context.Context, destination string) (net.Conn, error) {
	// Create a pipe to bridge queqiao and sing-box router
	pepConn, routerConn := net.Pipe()

	go p.handler.RouteConnection(ctx, routerConn, destination)

	return pepConn, nil
}

func (p *singboxDestinationPolicy) ResolveUDPAddr(ctx context.Context, destination string) ([]*net.UDPAddr, error) {
	// TODO: For UDP routing to work correctly with sing-box, we need to adapt 
	// Queqiao's server to use a custom PacketConn instead of net.ListenUDP.
	// For now, we return a loopback address as a placeholder or fallback to original behavior.
	return nil, nil
}
