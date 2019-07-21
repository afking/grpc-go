/*
 *
 * Copyright 2014 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package transport

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/internal/syscall"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type httpClient struct {
	nhc    *http.Client
	opts   ConnectOptions
	scheme string
	addr   TargetInfo

	onDial     sync.Once // Protects below fields
	authInfo   credentials.AuthInfo
	remoteAddr net.Addr
	localAddr  net.Addr

	perRPCCreds    []credentials.PerRPCCredentials
	transportCreds credentials.TransportCredentials
}

func (t *httpClient) dial(ctx context.Context, network, addr string) (conn net.Conn, err error) {
	if fn := t.opts.Dialer; fn != nil {
		conn, err = fn(ctx, addr)
	} else {
		conn, err = (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	if err != nil {
		if t.opts.FailOnNonTempDialError {
			return nil, connectionErrorf(isTemporary(err), err, "transport: error while dialing: %v", err)
		}
		return nil, connectionErrorf(true, err, "transport: error while dialing %v", err)
	}

	kp := t.opts.KeepaliveParams
	if kp.Time != infinity {
		if err := syscall.SetTCPUserTimeout(conn, kp.Timeout); err != nil {
			return nil, connectionErrorf(false, err, "transport: failed to set TCP_USER_TIMEOUT: %v", err)
		}
	}

	var authInfo credentials.AuthInfo
	if t.transportCreds != nil {
		// TODO: lock needed :(
		conn, t.authInfo, err = t.transportCreds.ClientHandshake(
			ctx, t.addr.Authority, conn,
		)
		if err != nil {
			return nil, connectionErrorf(isTemporary(err), err, "transport: authentication handshake failed: %v", err)
		}
	}

	t.onDial

	return
}

// newHTTP2Client constructs a connected ClientTransport to addr based on HTTP2
// and starts to receive messages on it. Non-nil error returns if construction
// fails.
func newHTTP2Client(
	connectCtx, ctx context.Context,
	addr TargetInfo,
	opts ConnectOptions,
	onPrefaceReceipt func(),
	onGoAway func(GoAwayReason),
	onClose func(),
) (*httpClient, error) {

	kp := opts.KeepaliveParams
	// Validate keepalive parameters.
	if kp.Time == 0 {
		kp.Time = defaultClientKeepaliveTime
	}
	if kp.Timeout == 0 {
		kp.Timeout = defaultClientKeepaliveTimeout
	}
	disableKeepAlives := true
	if kp.Time != infinity {
		disableKeepAlives = false
	}

	transportCreds := opts.TransportCredentials
	perRPCCreds := opts.PerRPCCredentials
	if b := opts.CredsBundle; b != nil {
		if t := b.TransportCredentials(); t != nil {
			transportCreds = t
		}
		if t := b.PerRPCCredentials(); t != nil {
			perRPCCreds = append(perRPCCreds, t)
		}
	}

	scheme := "http"
	if transportCreds != nil {
		scheme = "https"
	}

	t := &httpClient{
		//nhc:    nhc,
		opts:   opts,
		scheme: scheme,
		addr:   addr,
	}
	tr := &http.Transport{
		DialContext:        t.dial,
		MaxIdleConns:       10,
		IdleConnTimeout:    kp.Timeout,
		DisableCompression: true,
		DisableKeepAlives:  disableKeepAlives,
	}

	if err := http2.ConfigureTransport(tr); err != nil {
		return nil, err
	}
	t.nhc = &http.Client{
		Transport: tr,
	}
	return t, nil
}

func (t *httpClient) GracefulClose() {
	go t.nhc.CloseIdleConnections()
}

func (t *httpClient) Close() error {
	t.nhc.CloseIdleConnections()
	return nil
}

func (t *httpClient) createAudience(callHdr *CallHdr) string {
	// Create an audience string only if needed.
	if len(t.perRPCCreds) == 0 && callHdr.Creds == nil {
		return ""
	}
	// Construct URI required to get auth request metadata.
	// Omit port if it is the default one.
	host := strings.TrimSuffix(callHdr.Host, ":443")
	pos := strings.LastIndex(callHdr.Method, "/")
	if pos == -1 {
		pos = len(callHdr.Method)
	}
	return "https://" + host + callHdr.Method[:pos]
}

func (t *httpClient) setTrAuthData(ctx context.Context, hdr http.Header) error {
	for _, c := range t.perRPCCreds {
		data, err := c.GetRequestMetadata(ctx, audience)
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}

			return status.Errorf(codes.Unauthenticated, "transport: %v", err)
		}
		for k, v := range data {
			// Capital header names are illegal in HTTP/2.
			hdr.Set(strings.ToLower(k), encodeMetadataHeader(k, v))
		}
	}
	return nil
}

func (t *httpClient) setCallAuthData(ctx context.Context, audience string, callHdr *CallHdr, hdr http.Header) error {
	// Check if credentials.PerRPCCredentials were provided via call options.
	// Note: if these credentials are provided both via dial options and call
	// options, then both sets of credentials will be applied.
	if callCreds := callHdr.Creds; callCreds != nil {
		if !t.isSecure && callCreds.RequireTransportSecurity() {
			return status.Error(codes.Unauthenticated, "transport: cannot send secure credentials on an insecure connection")
		}
		data, err := callCreds.GetRequestMetadata(ctx, audience)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "transport: %v", err)
		}
		for k, v := range data {
			// Capital header names are illegal in HTTP/2
			hdr.Set(strings.ToLower(k), encodeMetadataHeader(k, v))
		}
	}
	return nil
}

func (t *httpClient) NewStream(ctx context.Context, callHdr *CallHdr) (*Stream, error) {
	pr := &peer.Peer{
		Addr: t.remoteAddr,
	}
	// Attach Auth info if there is any.
	if t.authInfo != nil {
		pr.AuthInfo = t.authInfo
	}

	ctx = peer.NewContext(ctx, pr)

	u, err := url.Parse(callHdr.Method)
	if err != nil {
		return nil, err
	}

	// new Request
	pr, pw := io.Pipe()
	req := &http.Request{
		Method:     method,
		URL:        u,
		Proto:      "HTTP/2",
		ProtoMajor: 2,
		ProtoMinor: 0,
		Header:     make(Header),
		Body:       pr,
		Host:       callHdr.Host,
	}

	h := req.Header
	h.Set("content-type", contentType(callHrd.ContentSubtype))

	if callHdr.PreviousAttempts > 0 {
		h.Set("grpc-previous-rpc-attempts", strconv.Itoa(callHdr.PreviousAttempts))
	}
	if callHdr.SendCompress != "" {
		h.Set("grpc-encoding", callHdr.SendCompress)
	}
	if dl, ok := ctx.Deadline(); ok {
		// Send out timeout regardless its value. The server can detect timeout context by itself.
		// TODO(mmukhi): Perhaps this field should be updated when actually writing out to the wire.
		timeout := time.Until(dl)
		h.Set("grpc-timeout", encodeTimeout(timeout))
	}
	if err := t.setTrAuthData(ctx, hdr); err != nil {
		return nil, err
	}
	aud := t.createAudience(callHdr)
	if err := t.setCallAuthData(ctx, aud, callHdr, hdr); err != nil {
		return nil, err
	}

	if b := stats.OutgoingTags(ctx); b != nil {
		h.Set("grpc-tags-bin", encodeBinHeader(b))
	}
	if b := stats.OutgoingTrace(ctx); b != nil {
		h.Set("grpc-trace-bin", encodeBinHeader(b))
	}

	if md, added, ok := metadata.FromOutgoingContextRaw(ctx); ok {
		var k string
		for k, vv := range md {
			// HTTP doesn't allow you to set pseudoheaders after non pseudoheaders were set.
			if isReservedHeader(k) {
				continue
			}

			for _, v := range vv {
				h[k] = append(h[k], encodeMetadataHeader(k, v))
			}
		}
		for _, vv := range added {
			for i, v := range vv {
				if i%2 == 0 {
					k = v
					continue
				}
				// HTTP doesn't allow you to set pseudoheaders after non pseudoheaders were set.
				if isReservedHeader(k) {
					continue
				}
				lk := strings.ToLower(k)
				h[lk] = append(h[lk], encodeMetadataHeader(k, v))
			}
		}
	}
	if md, ok := t.md.(*metadata.MD); ok {
		for k, vv := range *md {
			if isReservedHeader(k) {
				continue
			}
			for _, v := range vv {
				h[k] = append(h[k], encodeMetadataHeader(k, v))
			}
		}
	}

	res, err := t.nhc.Do(req)
	if err != nil {
		return nil, err
	}

	s := &Stream{
		ctx:            ctx,
		cancel:         cancel,
		ct:             t,
		method:         u.Path,
		sendCompress:   h.Get("grpc-encoding"),
		contentSubtype: h.Get("content-type"),
		remoteAddr:     t.RemoteAddr,
	}

	s.write = func(hdr, data []byte, opt *Options) error {
		if _, err := pw.Write(hdr); err != nil {
			return err
		}
		if _, err := pw.Write(data); err != nil {
			return err
		}
		if opt.Last {
			return pw.Close()
		}
		return nil
	}

	s.read = func(p []byte) (n int, err error) {
		n, err = res.Body.Read(p)
		if err == nil {
			return
		}
		if err != io.EOF {
			return
		}

		t := resp.Trailer

	}
	s.close = func() error {
		return res.Body.Close()
	}
	/*s.read = func(p []byte) (int, error) {
		return res.Body.Read(p)
	}*/

	if statsHdlr := t.opts.StatsHandler; statsHdlr != nil {
		outHeader := &stats.OutHeader{
			Client:      true,
			FullMethod:  callHdr.Method,
			RemoteAddr:  t.remoteAddr,
			LocalAddr:   t.localAddr,
			Compression: callHdr.SendCompress,
		}
		statsHdlr.HandleRPC(s.ctx, outHeader)
	}

}

func isTemporary(err error) bool {
	switch err := err.(type) {
	case interface {
		Temporary() bool
	}:
		return err.Temporary()
	case interface {
		Timeout() bool
	}:
		// Timeouts may be resolved upon retry, and are thus treated as
		// temporary.
		return err.Timeout()
	}
	return true
}
