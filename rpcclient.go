/*
RpcClient for Go RPC Servers
Copyright (C) ITsysCOM GmbH

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package rpcclient

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/syslog"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cgrates/birpc"
	"github.com/cgrates/birpc/context"
	"github.com/cgrates/birpc/jsonrpc"
)

// Constants to define the codec for RpcClient
const (
	JSONrpc     = "*json"
	HTTPjson    = "*http_jsonrpc"
	GOBrpc      = "*gob"
	InternalRPC = "*internal"

	BiRPCJSON     = "*birpc_json"
	BiRPCGOB      = "*birpc_gob"
	BiRPCInternal = "*birpc_internal"
)

// Constants to define the strategy for RpcClientPool
const (
	PoolFirst              = "*first"
	PoolAsync              = "*async"
	PoolRandom             = "*random"
	PoolNext               = "*next"
	PoolFirstPositive      = "*first_positive"
	PoolFirstPositiveAsync = "*first_positive_async"
	PoolParallel           = "*parallel"
	PoolBroadcast          = "*broadcast"
	PoolBroadcastSync      = "*broadcast_sync"
	PoolBroadcastAsync     = "*broadcast_async"
)

// Errors that library may return back
var (
	ErrReqUnsynchronized       = errors.New("REQ_UNSYNCHRONIZED")
	ErrUnsupporteServiceMethod = errors.New("UNSUPPORTED_SERVICE_METHOD")
	ErrWrongArgsType           = errors.New("WRONG_ARGS_TYPE")
	ErrWrongReplyType          = errors.New("WRONG_REPLY_TYPE")
	ErrDisconnected            = errors.New("DISCONNECTED")
	ErrReplyTimeout            = errors.New("REPLY_TIMEOUT")
	ErrFailedReconnect         = errors.New("FAILED_RECONNECT")
	ErrInternallyDisconnected  = errors.New("INTERNALLY_DISCONNECTED")
	ErrUnsupportedCodec        = errors.New("UNSUPPORTED_CODEC")
	ErrSessionNotFound         = errors.New("SESSION_NOT_FOUND")
	ErrPartiallyExecuted       = errors.New("PARTIALLY_EXECUTED")
	ErrUnsupportedBiRPC        = errors.New("UNSUPPORTED_BIRPC")
)

var logger logWriter

type logWriter interface {
	Alert(m string) error
	Close() error
	Crit(m string) error
	Debug(m string) error
	Emerg(m string) error
	Err(m string) error
	Info(m string) error
	Notice(m string) error
	Warning(m string) error
	Write(b []byte) (int, error)
}

func init() {
	logger, _ = syslog.New(syslog.LOG_INFO, "RPCClient") // If we need to report anything to syslog
}

// NewRPCClient creates a client based on the config
func NewRPCClient(ctx *context.Context, transport, addr string, tls bool, keyPath, certPath,
	caPath string, connectAttempts, reconnects int, maxReconnectInterval time.Duration,
	delayFunc func(time.Duration, time.Duration) func() time.Duration, connTimeout,
	replyTimeout time.Duration, codec string, internalChan chan birpc.ClientConnector,
	lazyConnect bool, biRPCClient interface{}) (rpcClient *RPCClient, err error) {
	switch codec {
	case InternalRPC:
		if reflect.ValueOf(internalChan).IsNil() {
			err = ErrInternallyDisconnected
			return
		}
	case BiRPCJSON, BiRPCGOB, BiRPCInternal:
		if codec == BiRPCInternal &&
			reflect.ValueOf(internalChan).IsNil() {
			err = ErrInternallyDisconnected
			return
		}
		if biRPCClient == nil {
			err = ErrUnsupportedBiRPC
			return
		}
	case JSONrpc, HTTPjson, GOBrpc:
	default:
		err = ErrUnsupportedCodec
		return
	}
	rpcClient = &RPCClient{
		transport:            transport,
		tls:                  tls,
		address:              addr,
		keyPath:              keyPath,
		certPath:             certPath,
		caPath:               caPath,
		reconnects:           reconnects,
		maxReconnectInterval: maxReconnectInterval,
		connTimeout:          connTimeout,
		replyTimeout:         replyTimeout,
		delayFunc:            delayFunc,
		codec:                codec,
		internalChan:         internalChan,
		biRPCClient:          biRPCClient,
	}
	if lazyConnect {
		return
	}
	delay := rpcClient.delayFunc(10*time.Millisecond, 0)
	for i := 0; i < connectAttempts; i++ {
		connCtx, cancel := context.WithTimeout(ctx, rpcClient.connTimeout)
		err = rpcClient.connect(connCtx)
		cancel()        // cancel ctx so we do not have any goroutine hanging
		if err != nil { // Cound not connect, retry
			time.Sleep(delay())
			continue
		}
		return // No error on connect, succcess
	}
	return
}

// RPCClient implements ClientConnector
type RPCClient struct {
	transport            string
	tls                  bool
	address              string
	keyPath              string
	certPath             string
	caPath               string
	reconnects           int
	maxReconnectInterval time.Duration
	connTimeout          time.Duration
	replyTimeout         time.Duration
	delayFunc            func(time.Duration, time.Duration) func() time.Duration // used to create/reset the delay function
	codec                string                                                  // JSONrpc or GOBrpc
	connection           birpc.ClientConnector
	connMux              sync.RWMutex // protects connection
	internalChan         chan birpc.ClientConnector
	biRPCClient          interface{}
}

func loadTLSConfig(clientCrt, clientKey, caPath string) (config *tls.Config, err error) {
	var cert tls.Certificate
	if clientCrt != "" && clientKey != "" {
		if cert, err = tls.LoadX509KeyPair(clientCrt, clientKey); err != nil {
			logger.Crit(fmt.Sprintf("Error: %s when load client cert and key", err))
			return
		}
	}

	var rootCAs *x509.CertPool
	if rootCAs, err = x509.SystemCertPool(); err != nil {
		logger.Crit(fmt.Sprintf("Error: %s when load SystemCertPool", err))
		return
	}

	if caPath != "" {
		var ca []byte
		if ca, err = os.ReadFile(caPath); err != nil {
			logger.Crit(fmt.Sprintf("Error: %s when read CA", err))
			return
		}

		if !rootCAs.AppendCertsFromPEM(ca) {
			logger.Crit("Cannot append certificate authority")
			return
		}
	}

	config = &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
	}
	return
}

func (client *RPCClient) connect(ctx *context.Context) (err error) {
	client.connMux.Lock()
	defer client.connMux.Unlock()
	var newClient func(conn io.ReadWriteCloser) birpc.ClientConnector
	switch client.codec {
	case InternalRPC:
		if client.connection != nil {
			return
		}
		select {
		case client.connection = <-client.internalChan:
			client.internalChan <- client.connection
			if client.connection == nil {
				return ErrDisconnected
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		return
	case HTTPjson:
		if client.tls {
			var config *tls.Config
			if config, err = loadTLSConfig(client.certPath, client.keyPath, client.caPath); err != nil {
				return
			}
			client.connection = &HTTPjsonRPCClient{
				httpClient: &http.Client{
					Transport: &http.Transport{
						TLSClientConfig: config,
					},
				},
				url: client.address,
			}
			return
		}
		client.connection = &HTTPjsonRPCClient{httpClient: new(http.Client), url: client.address}
		return
	case JSONrpc:
		newClient = func(conn io.ReadWriteCloser) birpc.ClientConnector { return jsonrpc.NewClient(conn) }
	case GOBrpc:
		newClient = func(conn io.ReadWriteCloser) birpc.ClientConnector { return birpc.NewClient(conn) }
	case BiRPCInternal:
		if client.connection != nil {
			return
		}
		select {
		case client.connection = <-client.internalChan:
			client.internalChan <- client.connection
			if client.connection == nil {
				return ErrDisconnected
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		return
	case BiRPCJSON:
		newClient = func(conn io.ReadWriteCloser) birpc.ClientConnector {
			c := birpc.NewBirpcClientWithCodec(jsonrpc.NewJSONBirpcCodec(conn))
			c.Register(client.biRPCClient)
			return c
		}
	case BiRPCGOB:
		newClient = func(conn io.ReadWriteCloser) birpc.ClientConnector {
			c := birpc.NewBirpcClient(conn)
			c.Register(client.biRPCClient)
			return c
		}

	}
	var netconn io.ReadWriteCloser
	if netconn, err = client.newNetConn(); err != nil {
		// logger.Crit(fmt.Sprintf("Error: %s when dialing", err.Error()))
		client.connection = nil // So we don't wrap nil into the interface
		return
	}
	client.connection = newClient(netconn)
	return
}

func (client *RPCClient) newNetConn() (netconn io.ReadWriteCloser, err error) {
	if client.tls {
		var config *tls.Config
		if config, err = loadTLSConfig(client.certPath, client.keyPath, client.caPath); err != nil {
			return
		}
		return tls.Dial(client.transport, client.address, config)
	}
	// RPC compliant connections here, manually create connection to timeout
	return net.DialTimeout(client.transport, client.address, client.connTimeout)
}

// getConn returns the connection
func (client *RPCClient) getConn() (c birpc.ClientConnector) {
	client.connMux.RLock()
	c = client.connection // the operations on connections need to be thread safe
	client.connMux.RUnlock()
	return
}

func (client *RPCClient) isConnected() bool {
	return client.getConn() != nil
}

func (client *RPCClient) disconnect() (err error) {
	switch client.codec {
	case InternalRPC, HTTPjson, BiRPCInternal:
	default:
		client.connMux.Lock()
		if client.connection != nil {
			client.connection.(io.Closer).Close()
			client.connection = nil
		}
		client.connMux.Unlock()
	}
	return
}

func (client *RPCClient) reconnect(ctx *context.Context) (err error) {
	client.disconnect()           // make sure we have cleared the connection so it can be garbage collected
	if client.codec == HTTPjson { // http client has automatic reconnects in place
		return client.connect(ctx)
	}
	delay := client.delayFunc(10*time.Millisecond, client.maxReconnectInterval)
	for i := 1; client.reconnects == -1 || i <= client.reconnects; i++ { // Maximum reconnects reached, -1 for infinite reconnects
		ctx2, cancel := context.WithTimeout(ctx, client.connTimeout)
		err = client.connect(ctx2)
		cancel()        // cancel ctx2 so we do not have any goroutine hanging
		if err != nil { // Cound not reconnect, retry
			tm := time.NewTimer(delay())
			select {
			case <-ctx.Done(): // it was called from Call so we have a reply timeout
				tm.Stop() // stop the timer
				return ctx.Err()
			case <-tm.C:
				continue
			}
		}
		return // No error on connect, succcess
	}
	return ErrFailedReconnect
}

// Call the method needed to implement ClientConnector
func (client *RPCClient) Call(ctx *context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	if args == nil || reply == nil || reflect.ValueOf(reply).IsNil() { // panics  on zero Value if not checked
		return fmt.Errorf("nil rpc in argument method: %s in: %v out: %v", serviceMethod, args, reply)
	}
	ctx2, cancel := context.WithTimeout(ctx, client.replyTimeout)
	defer cancel()
	if client.codec == InternalRPC || client.codec == BiRPCInternal { // only try to clone on *internal for the rest the arguments are marshaled so no clone needed
		if argsClnIface, clnable := args.(RPCCloner); clnable { // try cloning to avoid concurrency
			if args, err = argsClnIface.RPCClone(); err != nil {
				return
			}
		}
	}
	if err = client.call(ctx2, serviceMethod, args, reply); !IsConnectionErr(err) &&
		!IsServiceErr(err) ||
		err == context.DeadlineExceeded ||
		err.Error() == ErrSessionNotFound.Error() ||
		client.reconnects == 0 {
		return
	}
	if IsConnectionErr(err) {
		if errReconnect := client.reconnect(ctx); errReconnect != nil {
			return
		}
	}
	return client.call(ctx2, serviceMethod, args, reply)
}

func (client *RPCClient) call(ctx *context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	conn := client.getConn() // only protect to take the correct connection not to block until the operation on connection is done
	if conn == nil {
		return ErrDisconnected
	}
	return conn.Call(ctx, serviceMethod, args, reply)
}

// RPCCloner is an interface for objects to clone parts of themselves which are affected by concurrency at the time of RPC call
type RPCCloner interface {
	RPCClone() (interface{}, error)
}

// JSONrpcResponse is the response received from JSON RPC
type JSONrpcResponse struct {
	ID     uint64
	Result *json.RawMessage
	Error  interface{}
}

// HTTPjsonRPCClient only for the rpc over http
type HTTPjsonRPCClient struct {
	httpClient *http.Client
	id         uint64
	url        string
}

// Call the method needed to implement ClientConnector
func (client *HTTPjsonRPCClient) Call(ctx *context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	client.id++
	id := client.id
	var data []byte
	if data, err = json.Marshal(map[string]interface{}{
		"method": serviceMethod,
		"id":     client.id,
		"params": [1]interface{}{args},
	}); err != nil {
		return
	}

	var req *http.Request
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, client.url, io.NopCloser(bytes.NewBuffer(data)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	var resp *http.Response
	if resp, err = client.httpClient.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()
	var jsonRsp JSONrpcResponse
	if err = json.NewDecoder(resp.Body).Decode(&jsonRsp); err != nil {
		return
	}
	if jsonRsp.ID != id {
		return ErrReqUnsynchronized
	}
	if jsonRsp.Error != nil || jsonRsp.Result == nil {
		x, ok := jsonRsp.Error.(string)
		if !ok {
			return fmt.Errorf("invalid error %v", jsonRsp.Error)
		}
		if x == "" {
			x = "unspecified error"
		}
		return errors.New(x)
	}
	return json.Unmarshal(*jsonRsp.Result, reply)
}

// RPCPool is a pool of connections
type RPCPool struct {
	transmissionType string
	connections      []birpc.ClientConnector
	counter          int
	replyTimeout     time.Duration
}

// NewRPCPool creates RPCPool
func NewRPCPool(transmissionType string, replyTimeout time.Duration) *RPCPool {
	return &RPCPool{
		transmissionType: transmissionType,
		replyTimeout:     replyTimeout,
	}
}

// AddClient adds a client connection in the pool
func (pool *RPCPool) AddClient(rcc birpc.ClientConnector) {
	if rcc != nil && !reflect.ValueOf(rcc).IsNil() {
		pool.connections = append(pool.connections, rcc)
	}
}

// Call the method needed to implement birpc.ClientConnector
func (pool *RPCPool) Call(ctx *context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	switch pool.transmissionType {
	case PoolBroadcast:
		replyChan := make(chan *rpcReplyError, len(pool.connections))
		ctx2, cancel := context.WithTimeout(ctx, pool.replyTimeout)
		defer cancel()
		var wg sync.WaitGroup
		for _, rc := range pool.connections {
			wg.Add(1)
			go func(conn birpc.ClientConnector) {
				// make a new pointer of the same type
				rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface()))
				err := conn.Call(ctx2, serviceMethod, args, rpl.Interface())
				if !IsConnectionErr(err) && !IsServiceErr(err) {
					replyChan <- &rpcReplyError{reply: rpl.Interface(), err: err}
				}
				wg.Done()
			}(rc)
		}
		// in case each client returns an NetworkError
		// wait until all calls ended not until timeout
		allConnsEnded := make(chan struct{})
		go func() {
			wg.Wait()
			close(allConnsEnded)
		}()
		// get first response with timeout
		var re *rpcReplyError
		select {
		case re = <-replyChan:
		case <-allConnsEnded:
			return ErrDisconnected
		}
		// put received value in the orig reply
		reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(re.reply).Elem())
		return re.err
	case PoolBroadcastAsync:
		for _, rc := range pool.connections {
			go func(conn birpc.ClientConnector) {
				// make a new pointer of the same type
				rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface()))
				conn.Call(ctx, serviceMethod, args, rpl.Interface())
			}(rc)
		}
		return nil
	case PoolFirst:
		for _, rc := range pool.connections {
			err = rc.Call(ctx, serviceMethod, args, reply)
			if !IsConnectionErr(err) && !IsServiceErr(err) {
				return
			}
		}
	case PoolAsync:
		go func() {
			// because the call is async we need to copy the reply to avoid overwrite
			rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface()))
			for _, rc := range pool.connections {
				err := rc.Call(ctx, serviceMethod, args, rpl.Interface())
				if !IsConnectionErr(err) && !IsServiceErr(err) {
					return
				}
			}
		}()
	case PoolNext:
		ln := len(pool.connections)
		rrIndexes := roundIndex(int(math.Mod(float64(pool.counter), float64(ln))), ln)
		pool.counter++
		for _, index := range rrIndexes {
			err = pool.connections[index].Call(ctx, serviceMethod, args, reply)
			if !IsConnectionErr(err) && !IsServiceErr(err) {
				return
			}
		}
	case PoolRandom:
		rand.Seed(time.Now().UnixNano())
		randomIndex := rand.Perm(len(pool.connections))
		for _, index := range randomIndex {
			err = pool.connections[index].Call(ctx, serviceMethod, args, reply)
			if IsConnectionErr(err) || IsServiceErr(err) {
				continue
			}
			return
		}
	case PoolFirstPositive:
		for _, rc := range pool.connections {
			err = rc.Call(ctx, serviceMethod, args, reply)
			if err == nil {
				break
			}
		}
		return

	case PoolFirstPositiveAsync:
		rplyChan := make(chan interface{}, len(pool.connections))
		errChan := make(chan error, len(pool.connections))
		for _, rc := range pool.connections {
			go func(conn birpc.ClientConnector) {
				// make a new pointer of the same type
				rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface()))
				err := conn.Call(ctx, serviceMethod, args, rpl.Interface())
				if err != nil {
					errChan <- err
				} else {
					rplyChan <- rpl.Interface()
				}
			}(rc)

		}
		select {
		case rply := <-rplyChan: // first positive wins
			reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(rply).Elem())
			return
		case <-time.After(pool.replyTimeout): // no positive replied, return first error
			err = <-errChan
			return
		}
	case PoolBroadcastSync:
		// if all are succesfuly run the error is nil and result is populated
		// if all are network errors return the last network error
		// in rest return ErrPartiallyExecuted
		var wg sync.WaitGroup
		errChan := make(chan error, len(pool.connections))
		for _, rc := range pool.connections {
			wg.Add(1)
			go func(conn birpc.ClientConnector) {
				// make a new pointer of the same type
				rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface()))
				err := conn.Call(ctx, serviceMethod, args, rpl.Interface())
				if err != nil {
					errChan <- err
				} else {
					reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(rpl.Interface()).Elem())
				}
				wg.Done()
			}(rc)
		}
		wg.Wait() // wait for synchronous replication to finish
		close(errChan)
		// have errors but not all Calls failed
		if len(errChan) != 0 && len(errChan) < len(pool.connections) {
			return ErrPartiallyExecuted
		}
		// if all calls failed check if all are network errors
		for err = range errChan {
			if !IsConnectionErr(err) && !IsServiceErr(err) {
				logger.Warning(fmt.Sprintf("Error <%s> when calling <%s>", err, serviceMethod))
				return ErrPartiallyExecuted
			}
		}
	}
	return
}

type rpcReplyError struct {
	reply interface{}
	err   error
}

// generates round robin indexes for a slice of length max
// starting from index start
func roundIndex(start, max int) (result []int) {
	if start < 0 {
		start = 0
	}
	result = make([]int, max)
	for i := 0; i < max; i++ {
		if start+i < max {
			result[i] = start + i
		} else {
			result[i] = int(math.Abs(float64(max - (start + i))))
		}
	}
	return
}

// IsConnectionErr will decide if an error is network generated or not
func IsConnectionErr(err error) bool {
	if err == nil {
		return false
	}
	if _, isNetError := err.(*net.OpError); isNetError { // connection reset
		return true
	}
	if _, isDNSError := err.(*net.DNSError); isDNSError {
		return true
	}
	return err.Error() == birpc.ErrShutdown.Error() ||
		err.Error() == ErrReqUnsynchronized.Error() ||
		err.Error() == ErrDisconnected.Error() ||
		strings.HasSuffix(err.Error(), "connect: connection refused")
}

// IsServiceErr will decide if an error is a RPC one or not
func IsServiceErr(err error) bool {
	if err == nil {
		return false
	}

	return err.Error() == birpc.ErrShutdown.Error() ||
		err.Error() == ErrReplyTimeout.Error() ||
		strings.HasPrefix(err.Error(), "rpc: can't find service")
}

// NewRPCParallelClientPool returns a new RPCParallelClientPool
func NewRPCParallelClientPool(ctx *context.Context, transport, addr string, tls bool, keyPath,
	certPath, caPath string, connectAttempts, reconnects int, maxReconnectInterval time.Duration,
	delayFunc func(time.Duration, time.Duration) func() time.Duration, connTimeout,
	replyTimeout time.Duration, codec string, internalChan chan birpc.ClientConnector, maxCounter int64,
	initConns bool, biRPCClient interface{}) (rpcClient *RPCParallelClientPool, err error) {
	if codec != InternalRPC && codec != JSONrpc && codec != HTTPjson && codec != GOBrpc {
		err = ErrUnsupportedCodec
		return
	}
	if codec == InternalRPC && reflect.ValueOf(internalChan).IsNil() {
		err = ErrInternallyDisconnected
		return
	}
	rpcClient = &RPCParallelClientPool{
		transport:            transport,
		tls:                  tls,
		address:              addr,
		keyPath:              keyPath,
		certPath:             certPath,
		caPath:               caPath,
		connectAttempts:      connectAttempts,
		reconnects:           reconnects,
		maxReconnectInterval: maxReconnectInterval,
		connTimeout:          connTimeout,
		replyTimeout:         replyTimeout,
		delayFunc:            delayFunc,
		codec:                codec,
		internalChan:         internalChan,
		biRPCClient:          biRPCClient,

		connectionsChan: make(chan birpc.ClientConnector, maxCounter),
	}
	if initConns {
		err = rpcClient.initConns(ctx)
	}
	return
}

// RPCParallelClientPool implements ClientConnector
type RPCParallelClientPool struct {
	transport            string
	tls                  bool
	address              string
	keyPath              string
	certPath             string
	caPath               string
	connectAttempts      int
	reconnects           int
	maxReconnectInterval time.Duration
	connTimeout          time.Duration
	replyTimeout         time.Duration
	delayFunc            func(time.Duration, time.Duration) func() time.Duration
	codec                string // JSONrpc or GOBrpc
	internalChan         chan birpc.ClientConnector
	biRPCClient          interface{}

	connectionsChan chan birpc.ClientConnector
	counterMux      sync.RWMutex
	counter         int64
}

// Call the method needed to implement birpc.ClientConnector
func (pool *RPCParallelClientPool) Call(ctx *context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	var conn birpc.ClientConnector
	select {
	case conn = <-pool.connectionsChan:
	default:
		pool.counterMux.Lock()

		if pool.counter >= int64(cap(pool.connectionsChan)) {
			pool.counterMux.Unlock()
			conn = <-pool.connectionsChan
		} else {
			pool.counter++
			pool.counterMux.Unlock()
			if conn, err = NewRPCClient(ctx, pool.transport, pool.address, pool.tls,
				pool.keyPath, pool.certPath, pool.caPath, pool.connectAttempts, pool.reconnects,
				pool.maxReconnectInterval, pool.delayFunc, pool.connTimeout, pool.replyTimeout,
				pool.codec, pool.internalChan, false, pool.biRPCClient); err != nil {
				pool.counterMux.Lock()
				pool.counter-- // remove the conter if the connection was never created
				pool.counterMux.Unlock()
				return
			}
		}
	}
	err = conn.Call(ctx, serviceMethod, args, reply)
	pool.connectionsChan <- conn
	return
}

func (pool *RPCParallelClientPool) initConns(ctx *context.Context) (err error) {
	for pool.counter = 0; pool.counter < int64(cap(pool.connectionsChan)); pool.counter++ {
		var conn birpc.ClientConnector
		if conn, err = NewRPCClient(ctx, pool.transport, pool.address, pool.tls,
			pool.keyPath, pool.certPath, pool.caPath, pool.connectAttempts, pool.reconnects,
			pool.maxReconnectInterval, pool.delayFunc, pool.connTimeout, pool.replyTimeout,
			pool.codec, pool.internalChan, false, pool.biRPCClient); err != nil {
			return
		}
		pool.connectionsChan <- conn
	}
	return
}
