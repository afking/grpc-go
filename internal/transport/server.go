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
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/http2"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	//"google.golang.org/protobuf/proto"
)

var (
	// ErrIllegalHeaderWrite indicates that setting header is illegal because of
	// the stream's state.
	ErrIllegalHeaderWrite = errors.New("transport: the stream is done or WriteHeader was already called")
	// ErrHeaderListSizeLimitViolation indicates that the header list size is larger
	// than the limit set by peer.
	ErrHeaderListSizeLimitViolation = errors.New("transport: trying to send header list size larger than the limit set by peer")
	// statusRawProto is a function to get to the raw status proto wrapped in a
	// status.Status without a proto.Clone().
	statusRawProto = internal.StatusRawProto.(func(*status.Status) *spb.Status)
)

// httpServer implements the ServerTransport interface with native http server.
type httpServer struct {
	nhs     *http.Server
	handler Handler
	config  *ServerConfig

	/*ctx        context.Context
	conn       net.Conn
	remoteAddr net.Addr
	localAddr  net.Addr*/

	// Fields below are for channelz metric collection.
	channelzID int64 // channelz unique identification number
	czData     *channelzData
	bufferPool *bufferPool
}

func (t *httpServer) serveHTTP(w http.ResponseWriter, r *http.Request) error {
	fmt.Println("HERE", r)
	if r.ProtoMajor != 2 {
		return errors.New("gRPC requires HTTP/2")
	}
	if r.Method != "POST" {
		return errors.New("invalid gRPC request method")
	}
	defer r.Body.Close()

	f, ok := w.(http.Flusher)
	if !ok {
		return errors.New("gRPC requires a ResponseWriter supporting http.Flusher")
	}

	contentType := r.Header.Get("Content-Type")
	// TODO: do we assume contentType is lowercase? we did before
	contentSubtype, validContentType := contentSubtype(contentType)
	if !validContentType {
		return errors.New("invalid gRPC request content-type")
	}

	ctx := r.Context()
	var cancel context.CancelFunc
	if v := r.Header.Get("grpc-timeout"); v != "" {
		to, err := decodeTimeout(v)
		if err != nil {
			return status.Errorf(codes.Internal, "malformed time-out: %v", err)
		}

		ctx, cancel = context.WithTimeout(ctx, to)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	metakv := []string{"content-type", contentType}
	for k, vv := range r.Header {
		k = strings.ToLower(k)
		if isReservedHeader(k) && !isWhitelistedHeader(k) {
			continue
		}
		for _, v := range vv {
			v, err := decodeMetadataHeader(k, v)
			if err != nil {
				return status.Errorf(codes.Internal, "malformed binary metadata: %v", err)
			}
			metakv = append(metakv, k, v)
		}
	}
	hdrs := metadata.Pairs(metakv...)
	ctx = metadata.NewIncomingContext(ctx, hdrs)

	s := &Stream{
		//id:             0, // irrelevant
		//requestRead:    func(int) {},
		ctx:    ctx,
		cancel: cancel,
		//buf:            newRecvBuffer(),
		st:             t,
		method:         r.URL.Path,
		recvCompress:   r.Header.Get("grpc-encoding"),
		contentSubtype: contentSubtype,
		remoteAddr:     r.RemoteAddr,
	}
	remoteAddr := s.RemoteAddr()

	// configure peer
	pr := &peer.Peer{
		Addr: remoteAddr,
	}
	if r.TLS != nil {
		pr.AuthInfo = credentials.TLSInfo{State: *r.TLS}
	}
	s.ctx = peer.NewContext(s.ctx, pr)

	if traceCtx := t.config.TraceCtx; traceCtx != nil {
		s.ctx = traceCtx(s.ctx, s.method)
	}
	if statsHdlr := t.config.StatsHandler; statsHdlr != nil {
		tagInfo := &stats.RPCTagInfo{FullMethodName: s.method}
		s.ctx = statsHdlr.TagRPC(s.ctx, tagInfo)
		inHeader := &stats.InHeader{
			FullMethod:  s.method,
			RemoteAddr:  remoteAddr,
			Compression: s.recvCompress,
		}
		statsHdlr.HandleRPC(s.ctx, inHeader)
	}

	s.ctxDone = s.ctx.Done()

	s.read = r.Body
	//s.wq = newWriteQuota(defaultWriteQuota, s.ctxDone)
	/*s.trReader = &transportReader{
		reader: &recvBufferReader{
			ctx:        s.ctx,
			ctxDone:    s.ctxDone,
			recv:       s.buf,
			freeBuffer: t.bufferPool.put,
		},
	}*/
	/*// Register the stream with loopy.
	t.controlBuf.put(&registerStream{
		streamID: s.id,
		wq:       s.wq,
	})*/

	var wroteHeaders bool
	writeCommonHeaders := func() {
		if wroteHeaders {
			return
		}
		wroteHeaders = true

		h := w.Header()
		h["date"] = nil // suppress Date to make tests happy; TODO: restore
		h.Set("content-type", contentType)

		// Predeclare trailers we'll set later in WriteStatus (after the body).
		// This is a SHOULD in the HTTP RFC, and the way you add (known)
		// Trailers per the net/http.ResponseWriter contract.
		// See https://golang.org/pkg/net/http/#ResponseWriter
		// and https://golang.org/pkg/net/http/#example_ResponseWriter_trailers
		h.Add("trailer", "grpc-status")
		h.Add("trailer", "grpc-message")
		h.Add("trailer", "grpc-status-details-bin")

		if s.sendCompress != "" {
			h.Set("grpc-encoding", s.sendCompress)
		}
	}

	s.writeHeader = func(md metadata.MD) error {
		writeCommonHeaders()

		h := w.Header()
		for k, vv := range md {
			// Clients don't tolerate reading restricted headers after some non restricted ones were sent.
			if isReservedHeader(k) {
				continue
			}
			for _, v := range vv {
				v = encodeMetadataHeader(k, v)
				h.Add(k, v)
			}
		}
		w.WriteHeader(200)
		f.Flush()

		return nil
	}

	s.write = func(hdr, data []byte, opt *Options) error {
		writeCommonHeaders()

		if _, err := w.Write(hdr); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		f.Flush()
		return nil
	}

	s.writeStatus = func(st *status.Status) error {
		writeCommonHeaders()

		// And flush, in case no header or body has been sent yet.
		// This forces a separation of headers and trailers if this is the
		// first call (for example, in end2end tests's TestNoService).
		f.Flush()

		h := w.Header()
		h.Set("grpc-status", fmt.Sprintf("%d", st.Code()))
		if m := st.Message(); m != "" {
			h.Set("grpc-message", encodeGrpcMessage(m))
		}

		if p := st.Proto(); p != nil && len(p.Details) > 0 {
			stBytes, err := proto.Marshal(p)
			if err != nil {
				return err
			}

			h.Set("grpc-status-details-bin", encodeBinHeader(stBytes))
		}

		md := s.Trailer()
		for k, vv := range md {
			// Clients don't tolerate reading restricted headers after some non restricted ones were sent.
			if isReservedHeader(k) {
				continue
			}
			for _, v := range vv {
				// http2 ResponseWriter mechanism to send undeclared Trailers after
				// the headers have possibly been written.
				h.Add(http2.TrailerPrefix+k, encodeMetadataHeader(k, v))
			}
		}
		return nil
	}

	t.handler(t, s) // blocks
	return nil
}

func (t *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := t.serveHTTP(w, r); err != nil {
		// TODO: errors
		http.Error(w, err.Error(), 500)
	}
}

// newServer constructs a ServerTransport based on HTTP2. ConnectionError is
// returned if something goes wrong.
func newServer(handler Handler, config *ServerConfig) (ServerTransport, error) {
	fmt.Printf("%#v\n", config)
	nhs := &http.Server{}

	if err := http2.ConfigureServer(nhs, &http2.Server{
		MaxReadFrameSize: uint32(config.ReadBufferSize),
	}); err != nil {
		return nil, err
	}

	return &httpServer{
		nhs:     nhs,
		handler: handler,
		config:  config,
	}, nil

}

type credListener struct {
	net.Listener
	creds credentials.TransportCredentials
}

func (cl *credListener) Accept() (net.Conn, error) {
	c, err := cl.Listener.Accept()
	if err != nil {
		return nil, err
	}
	c, _, err = cl.creds.ServerHandshake(c)
	return c, err
}

func (t *httpServer) Serve(ls net.Listener) error {
	if t.config.Creds != nil {
		ls = &credListener{ls, t.config.Creds}
	}
	return t.nhs.Serve(ls)
}

func (t *httpServer) Close() error {
	return t.nhs.Close()
}

func (t *httpServer) Drain() {
	t.nhs.Shutdown(context.TODO())
}

/*func (t *httpServer) IncrMsgSent() {
	atomic.AddInt64(&t.czData.msgSent, 1)
	atomic.StoreInt64(&t.czData.lastMsgSentTime, time.Now().UnixNano())
}

func (t *httpServer) IncrMsgRecv() {
	atomic.AddInt64(&t.czData.msgRecv, 1)
	atomic.StoreInt64(&t.czData.lastMsgRecvTime, time.Now().UnixNano())
}*/

/*func (t *httpServer) ChannelzMetric() *channelz.SocketInternalMetric {
	s := channelz.SocketInternalMetric{
		StreamsStarted:                   atomic.LoadInt64(&t.czData.streamsStarted),
		StreamsSucceeded:                 atomic.LoadInt64(&t.czData.streamsSucceeded),
		StreamsFailed:                    atomic.LoadInt64(&t.czData.streamsFailed),
		MessagesSent:                     atomic.LoadInt64(&t.czData.msgSent),
		MessagesReceived:                 atomic.LoadInt64(&t.czData.msgRecv),
		KeepAlivesSent:                   atomic.LoadInt64(&t.czData.kpCount),
		LastRemoteStreamCreatedTimestamp: time.Unix(0, atomic.LoadInt64(&t.czData.lastStreamCreatedTime)),
		LastMessageSentTimestamp:         time.Unix(0, atomic.LoadInt64(&t.czData.lastMsgSentTime)),
		LastMessageReceivedTimestamp:     time.Unix(0, atomic.LoadInt64(&t.czData.lastMsgRecvTime)),
		LocalFlowControlWindow:           int64(t.fc.getSize()),
		SocketOptions:                    channelz.GetSocketOption(t.conn),
		LocalAddr:                        t.localAddr,
		RemoteAddr:                       t.remoteAddr,
		// RemoteName :
	}
	if au, ok := t.authInfo.(credentials.ChannelzSecurityInfo); ok {
		s.Security = au.GetSecurityValue()
	}
	s.RemoteFlowControlWindow = t.getOutFlowWindow()
	return &s
}*/
