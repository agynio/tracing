package ziticonn

import (
	"context"
	"net"
	"strings"

	"google.golang.org/grpc/peer"
)

type dialerIdentityConn interface {
	GetDialerIdentityId() string
}

type identityAddr struct {
	net.Addr
	identity string
}

func (a identityAddr) DialerIdentity() string {
	return a.identity
}

type identityConn struct {
	net.Conn
	remoteAddr net.Addr
}

func (c *identityConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

type wrappedListener struct {
	net.Listener
}

func WrapListener(listener net.Listener) net.Listener {
	return &wrappedListener{Listener: listener}
}

func (w *wrappedListener) Accept() (net.Conn, error) {
	conn, err := w.Listener.Accept()
	if err != nil {
		return nil, err
	}

	identity := dialerIdentityFromConn(conn)
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return conn, nil
	}

	return &identityConn{
		Conn:       conn,
		remoteAddr: identityAddr{Addr: conn.RemoteAddr(), identity: identity},
	}, nil
}

func SourceIdentityFromContext(ctx context.Context) (string, bool) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok || peerInfo == nil || peerInfo.Addr == nil {
		return "", false
	}

	identity, ok := dialerIdentityFromAddr(peerInfo.Addr)
	if !ok {
		return "", false
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return "", false
	}

	return identity, true
}

func dialerIdentityFromConn(conn net.Conn) string {
	if identityConn, ok := conn.(dialerIdentityConn); ok {
		return identityConn.GetDialerIdentityId()
	}
	return ""
}

func dialerIdentityFromAddr(addr net.Addr) (string, bool) {
	if identityAddr, ok := addr.(interface{ DialerIdentity() string }); ok {
		return identityAddr.DialerIdentity(), true
	}
	return "", false
}
