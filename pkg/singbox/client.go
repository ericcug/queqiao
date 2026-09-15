package singbox

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pep"
	"github.com/bojieli/queqiao/internal/socks5"
)

type ClientOptions struct {
	EnrollmentURI string
	ProfilePath   string
	PathProfile   string
	Transport     string
	RemoteAddr    string
	LocalAddress  string
}

type Client struct {
	pepClient *pep.Client
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// NewClient initializes the Queqiao client. If the profile does not exist,
// it uses the EnrollmentURI to enroll and generate the profile.
func NewClient(opts ClientOptions) (*Client, error) {
	var profile identity.ClientProfile
	var err error

	if fileExists(opts.ProfilePath) {
		profile, err = identity.LoadClientProfile(opts.ProfilePath)
		if err != nil {
			return nil, err
		}
	} else if opts.EnrollmentURI != "" {
		invite, err := identity.ParseInvitation(opts.EnrollmentURI, time.Now())
		if err != nil {
			return nil, err
		}
		profile, err = identity.Enroll(context.Background(), invite, "sing-box", 30*time.Second)
		if err != nil {
			return nil, err
		}
		if err := profile.SaveNew(opts.ProfilePath); err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("queqiao profile not found and enrollment URI not provided")
	}

	creds, err := profile.Credentials()
	if err != nil {
		return nil, err
	}

	pepCfg := pep.ClientConfig{
		RemoteAddr:   opts.RemoteAddr,
		LocalAddress: opts.LocalAddress,
		Credentials:  creds,
	}

	if opts.Transport != "" {
		pepCfg.Transport = pep.TransportKind(opts.Transport)
	}

	pepClient, err := pep.NewClient(pepCfg)
	if err != nil {
		return nil, err
	}

	pepClient.Start(context.Background())

	return &Client{
		pepClient: pepClient,
	}, nil
}

// DialConn opens a reliable connection to the destination.
func (c *Client) DialConn(ctx context.Context, destination string) (net.Conn, error) {
	return c.pepClient.DialConn(ctx, destination)
}

// ListenPacket creates a UDP association and returns a net.PacketConn adapter
// for sending and receiving raw payloads without SOCKS5 headers.
func (c *Client) ListenPacket(ctx context.Context) (net.PacketConn, error) {
	clientControl, queqiaoControl := net.Pipe()

	go func() {
		// handleUDPAssociate blocks until the association is closed.
		c.pepClient.HandleUDPAssociate(ctx, queqiaoControl)
	}()

	// The first message is a socks5 reply telling us the bound UDP address.
	reply, err := socks5.ReadReply(clientControl)
	if err != nil {
		clientControl.Close()
		return nil, err
	}
	if reply.Reply != socks5.ReplySucceeded {
		clientControl.Close()
		return nil, errors.New("SOCKS5 UDP associate failed")
	}

	// Dial the local UDP port assigned by HandleUDPAssociate
	udpConn, err := net.DialUDP("udp", nil, &net.UDPAddr{
		IP:   reply.Addr.IP,
		Port: reply.Addr.Port,
	})
	if err != nil {
		clientControl.Close()
		return nil, err
	}

	return &socks5UDPPacketConn{
		udpConn: udpConn,
		control: clientControl,
	}, nil
}

type socks5UDPPacketConn struct {
	udpConn *net.UDPConn
	control net.Conn // keep alive
}

func (c *socks5UDPPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	buf := make([]byte, 65536)
	n, _, err = c.udpConn.ReadFromUDP(buf)
	if err != nil {
		return 0, nil, err
	}
	datagram, err := socks5.ReadUDPDatagram(buf[:n])
	if err != nil {
		return 0, nil, err
	}
	n = copy(p, datagram.Payload)
	return n, fakeAddr{dest: datagram.Destination}, nil
}

func (c *socks5UDPPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	var buf bytes.Buffer
	err = socks5.WriteUDPDatagram(&buf, addr.String(), p)
	if err != nil {
		return 0, err
	}
	_, err = c.udpConn.Write(buf.Bytes())
	return len(p), err
}

func (c *socks5UDPPacketConn) Close() error {
	c.control.Close()
	return c.udpConn.Close()
}

func (c *socks5UDPPacketConn) LocalAddr() net.Addr {
	return c.udpConn.LocalAddr()
}

func (c *socks5UDPPacketConn) SetDeadline(t time.Time) error {
	return c.udpConn.SetDeadline(t)
}

func (c *socks5UDPPacketConn) SetReadDeadline(t time.Time) error {
	return c.udpConn.SetReadDeadline(t)
}

func (c *socks5UDPPacketConn) SetWriteDeadline(t time.Time) error {
	return c.udpConn.SetWriteDeadline(t)
}

type fakeAddr struct {
	dest string
}

func (a fakeAddr) Network() string { return "udp" }
func (a fakeAddr) String() string  { return a.dest }
