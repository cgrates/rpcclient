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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log/syslog"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Constants to define the codec for RpcClient
const (
	JSONrpc     = "*json"
	HTTPjson    = "*http_jsonrpc"
	GOBrpc      = "*gob"
	InternalRPC = "*internal"
)

// Constants to define the strategy for RpcClientPool
const (
	PoolFirst         = "*first"
	PoolRandom        = "*random"
	PoolNext          = "*next"
	PoolBroadcast     = "*broadcast"
	PoolFirstPositive = "*first_positive"
	PoolParallel      = "*parallel"
	PoolBroadcastSync = "*broadcast_sync"
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
)
var logger *syslog.Writer

func init() {
	logger, _ = syslog.New(syslog.LOG_INFO, "RPCClient") // If we need to report anything to syslog
}

// Fib returns successive Fibonacci numbers.
func Fib() func() time.Duration {
	a, b := 0, 1
	return func() time.Duration {
		a, b = b, a+b
		return time.Duration(a*10) * time.Millisecond
	}
}

// NewRPCClient creates a client based on the config
func NewRPCClient(transport, addr string, tls bool,
	keyPath, certPath, caPath string, connectAttempts, reconnects int,
	connTimeout, replyTimeout time.Duration, codec string,
	internalChan chan ClientConnector, lazyConnect bool) (rpcClient *RPCClient, err error) {
	if codec != InternalRPC && codec != JSONrpc && codec != HTTPjson && codec != GOBrpc {
		err = ErrUnsupportedCodec
		return
	}
	if codec == InternalRPC && reflect.ValueOf(internalChan).IsNil() {
		err = ErrInternallyDisconnected
		return
	}
	rpcClient = &RPCClient{
		transport:    transport,
		tls:          tls,
		address:      addr,
		keyPath:      keyPath,
		certPath:     certPath,
		caPath:       caPath,
		reconnects:   reconnects,
		connTimeout:  connTimeout,
		replyTimeout: replyTimeout,
		codec:        codec,
		internalChan: internalChan,
	}
	if lazyConnect {
		return
	}
	delay := Fib()
	for i := 0; i < connectAttempts; i++ {
		err = rpcClient.connect()
		if err == nil { //Connected so no need to reiterate
			break
		}
		time.Sleep(delay())
	}
	return
}

// RPCClient implements ClientConnector
type RPCClient struct {
	transport    string
	tls          bool
	address      string
	keyPath      string
	certPath     string
	caPath       string
	reconnects   int
	connTimeout  time.Duration
	replyTimeout time.Duration
	codec        string // JSONrpc or GOBrpc
	connection   ClientConnector
	connMux      sync.RWMutex // protects connection
	internalChan chan ClientConnector
}

func loadTLSConfig(clientCrt, clientKey, caPath string) (config *tls.Config, err error) {
	var cert tls.Certificate
	if clientCrt != "" && clientKey != "" {
		cert, err = tls.LoadX509KeyPair(clientCrt, clientKey)
		if err != nil {
			logger.Crit(fmt.Sprintf("Error: %s when load client cert and key", err))
			return
		}
	}

	var rootCAs *x509.CertPool
	if rootCAs, err = x509.SystemCertPool(); err != nil {
		logger.Crit(fmt.Sprintf("Error: %s when load SystemCertPool", err))
		return
	}
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	if caPath != "" {
		var ca []byte
		if ca, err = ioutil.ReadFile(caPath); err != nil {
			logger.Crit(fmt.Sprintf("Error: %s when read CA", err))
			return
		}

		if ok := rootCAs.AppendCertsFromPEM(ca); !ok {
			logger.Crit(fmt.Sprintf("Cannot append certificate authority"))
			return
		}
	}

	config = &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
	}
	return
}

func (client *RPCClient) connect() (err error) {
	client.connMux.Lock()
	defer client.connMux.Unlock()
	if client.codec == InternalRPC {
		if client.connection == nil {
			select {
			case client.connection = <-client.internalChan:
				client.internalChan <- client.connection
				if client.connection == nil {
					return ErrDisconnected
				}
			case <-time.After(client.connTimeout):
				return ErrDisconnected
			}
		}
		return
	}
	if client.codec == HTTPjson {
		if client.tls {
			var config *tls.Config
			if config, err = loadTLSConfig(client.certPath, client.keyPath, client.caPath); err != nil {
				return
			}
			transport := &http.Transport{TLSClientConfig: config}
			httpClient := &http.Client{Transport: transport}
			client.connection = &HTTPjsonRPCClient{httpClient: httpClient, url: client.address}
		} else {
			client.connection = &HTTPjsonRPCClient{httpClient: new(http.Client), url: client.address}
		}
		return
	}
	var netconn io.ReadWriteCloser
	if client.tls {
		var config *tls.Config
		if config, err = loadTLSConfig(client.certPath, client.keyPath, client.caPath); err != nil {
			return
		}
		if netconn, err = tls.Dial(client.transport, client.address, config); err != nil {
			logger.Crit(fmt.Sprintf("Error: %s when dialing", err.Error()))
			return
		}
	} else {
		// RPC compliant connections here, manually create connection to timeout
		if netconn, err = net.DialTimeout(client.transport, client.address, client.connTimeout); err != nil {
			client.connection = nil // So we don't wrap nil into the interface
			return
		}
	}

	if client.codec == JSONrpc {
		client.connection = jsonrpc.NewClient(netconn)
	} else {
		client.connection = rpc.NewClient(netconn)
	}
	return
}

func (client *RPCClient) isConnected() bool {
	client.connMux.RLock()
	defer client.connMux.RUnlock()
	return client.connection != nil
}

func (client *RPCClient) disconnect() (err error) {
	switch client.codec {
	case InternalRPC, HTTPjson:
	default:
		client.connMux.Lock()
		if client.connection != nil {
			client.connection.(*rpc.Client).Close()
			client.connection = nil
		}
		client.connMux.Unlock()
	}
	return
}

func (client *RPCClient) reconnect() (err error) {
	client.disconnect()           // make sure we have cleared the connection so it can be garbage collected
	if client.codec == HTTPjson { // http client has automatic reconnects in place
		return client.connect()
	}
	delay := Fib()
	for i := 1; client.reconnects == -1 || i <= client.reconnects; i++ { // Maximum reconnects reached, -1 for infinite reconnects
		if err = client.connect(); err != nil { // No error on connect, succcess
			time.Sleep(delay()) // Cound not reconnect, retry
			continue
		}
		return
	}
	return ErrFailedReconnect
}

// Call the method needed to implement ClientConnector
func (client *RPCClient) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	if args == nil || reply == nil || reflect.ValueOf(reply).IsNil() { // panics  on zero Value if not checked
		return fmt.Errorf("nil rpc in argument method: %s in: %v out: %v", serviceMethod, args, reply)
	}
	rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface())).Interface() // clone to avoid concurrency
	errChan := make(chan error, 1)
	go func(serviceMethod string, args interface{}, reply interface{}) {
		client.connMux.RLock()
		if client.connection == nil {
			errChan <- ErrDisconnected
		} else {
			if argsClnIface, clnable := args.(RPCCloner); clnable { // try cloning to avoid concurrency
				var argsCloned interface{}
				if argsCloned, err = argsClnIface.RPCClone(); err != nil {
					errChan <- err
					return
				}
				args = argsCloned
			}
			errChan <- client.connection.Call(serviceMethod, args, reply)
		}
		client.connMux.RUnlock()
	}(serviceMethod, args, rpl)
	select {
	case err = <-errChan:
	case <-time.After(client.replyTimeout):
		err = ErrReplyTimeout
	}
	if IsNetworkError(err) && err != ErrReplyTimeout &&
		err.Error() != ErrSessionNotFound.Error() &&
		client.reconnects != 0 { // ReplyTimeout should not reconnect since it creates loop
		if errReconnect := client.reconnect(); errReconnect != nil {
			return
		}
		client.connMux.RLock()
		defer client.connMux.RUnlock()
		return client.connection.Call(serviceMethod, args, reply)
	}
	reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(rpl).Elem()) // no errors, copy the reply from clone
	return
}

// ClientConnector is the connection used in RpcClient, as interface so we can combine the rpc.RpcClient with http one or websocket
type ClientConnector interface {
	Call(serviceMethod string, args interface{}, reply interface{}) (err error)
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
func (client *HTTPjsonRPCClient) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
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
	var resp *http.Response
	if resp, err = client.httpClient.Post(client.url, "application/json",
		ioutil.NopCloser(strings.NewReader(string(data)))); err != nil { // Closer so we automatically have close after response
		return
	}
	var body []byte
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	var jsonRsp JSONrpcResponse
	if err = json.Unmarshal(body, &jsonRsp); err != nil {
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
	connections      []ClientConnector
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
func (pool *RPCPool) AddClient(rcc ClientConnector) {
	if rcc != nil && !reflect.ValueOf(rcc).IsNil() {
		pool.connections = append(pool.connections, rcc)
	}
}

// Call the method needed to implement ClientConnector
func (pool *RPCPool) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	switch pool.transmissionType {
	case PoolBroadcast:
		replyChan := make(chan *rpcReplyError, len(pool.connections))
		for _, rc := range pool.connections {
			go func(conn ClientConnector) {
				// make a new pointer of the same type
				rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface()))
				err := conn.Call(serviceMethod, args, rpl.Interface())
				if !IsNetworkError(err) {
					replyChan <- &rpcReplyError{reply: rpl.Interface(), err: err}
				}
			}(rc)
		}
		//get first response with timeout
		var re *rpcReplyError
		select {
		case re = <-replyChan:
		case <-time.After(pool.replyTimeout):
			return ErrReplyTimeout
		}
		// put received value in the orig reply
		reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(re.reply).Elem())
		return re.err
	case PoolFirst:
		for _, rc := range pool.connections {
			err = rc.Call(serviceMethod, args, reply)
			if IsNetworkError(err) {
				continue
			}
			return
		}
	case PoolNext:
		ln := len(pool.connections)
		rrIndexes := roundIndex(int(math.Mod(float64(pool.counter), float64(ln))), ln)
		pool.counter++
		for _, index := range rrIndexes {
			err = pool.connections[index].Call(serviceMethod, args, reply)
			if IsNetworkError(err) {
				continue
			}
			return
		}
	case PoolRandom:
		rand.Seed(time.Now().UnixNano())
		randomIndex := rand.Perm(len(pool.connections))
		for _, index := range randomIndex {
			err = pool.connections[index].Call(serviceMethod, args, reply)
			if IsNetworkError(err) {
				continue
			}
			return
		}
	case PoolFirstPositive:
		for _, rc := range pool.connections {
			err = rc.Call(serviceMethod, args, reply)
			if err == nil {
				break
			}
		}
		return
	case PoolBroadcastSync:
		var wg sync.WaitGroup

		errChan := make(chan error, len(pool.connections))
		for _, rc := range pool.connections {
			wg.Add(1)
			go func(conn ClientConnector) {
				// make a new pointer of the same type
				rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface()))
				err := conn.Call(serviceMethod, args, rpl.Interface())
				if err != nil {
					errChan <- err
				} else {
					reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(rpl.Interface()).Elem())
				}
				wg.Done()
			}(rc)
		}
		wg.Wait() // wait for synchronous replication to finish
		var hasErr bool
		for exit := false; !exit; {
			select {
			case chanErr := <-errChan:
				if !IsNetworkError(chanErr) {
					logger.Warning(fmt.Sprintf("Error <%s> when calling <%s>", err, serviceMethod))
					hasErr = true
				} else {
					err = chanErr
				}
			default:
				exit = true
			}
		}
		if err == nil && hasErr {
			err = ErrPartiallyExecuted
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

func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if _, isNetError := err.(*net.OpError); isNetError {
		return true
	}
	if _, isDNSError := err.(*net.DNSError); isDNSError {
		return true
	}
	return err == rpc.ErrShutdown ||
		err == ErrReqUnsynchronized ||
		err == ErrDisconnected ||
		err == ErrReplyTimeout ||
		err.Error() == ErrSessionNotFound.Error() ||
		strings.HasPrefix(err.Error(), "rpc: can't find service")
}

// NewRPCParallelClientPool returns a new RPCParallelClientPool
func NewRPCParallelClientPool(transport, addr string, tls bool,
	keyPath, certPath, caPath string, connectAttempts, reconnects int,
	connTimeout, replyTimeout time.Duration, codec string,
	internalChan chan ClientConnector, maxCounter int64, initConns bool) (rpcClient *RPCParallelClientPool, err error) {
	if codec != InternalRPC && codec != JSONrpc && codec != HTTPjson && codec != GOBrpc {
		err = ErrUnsupportedCodec
		return
	}
	if codec == InternalRPC && reflect.ValueOf(internalChan).IsNil() {
		err = ErrInternallyDisconnected
		return
	}
	rpcClient = &RPCParallelClientPool{
		transport:       transport,
		tls:             tls,
		address:         addr,
		keyPath:         keyPath,
		certPath:        certPath,
		caPath:          caPath,
		connectAttempts: connectAttempts,
		reconnects:      reconnects,
		connTimeout:     connTimeout,
		replyTimeout:    replyTimeout,
		codec:           codec,
		internalChan:    internalChan,

		connectionsChan: make(chan ClientConnector, maxCounter),
	}
	if initConns {
		err = rpcClient.initConns()
	}
	return
}

// RPCParallelClientPool implements ClientConnector
type RPCParallelClientPool struct {
	transport       string
	tls             bool
	address         string
	keyPath         string
	certPath        string
	caPath          string
	reconnects      int
	connectAttempts int
	connTimeout     time.Duration
	replyTimeout    time.Duration
	codec           string // JSONrpc or GOBrpc
	internalChan    chan ClientConnector

	connectionsChan chan ClientConnector
	counterMux      sync.RWMutex
	counter         int64
}

// Call the method needed to implement ClientConnector
func (pool *RPCParallelClientPool) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	var conn ClientConnector
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
			if conn, err = NewRPCClient(pool.transport, pool.address, pool.tls,
				pool.keyPath, pool.certPath, pool.caPath, pool.connectAttempts, pool.reconnects,
				pool.connTimeout, pool.replyTimeout, pool.codec,
				pool.internalChan, false); err != nil {
				pool.counterMux.Lock()
				pool.counter-- // remove the conter if the connection was never created
				pool.counterMux.Unlock()
				return
			}
		}
	}
	err = conn.Call(serviceMethod, args, reply)
	pool.connectionsChan <- conn
	return
}

func (pool *RPCParallelClientPool) initConns() (err error) {
	for pool.counter = 0; pool.counter < int64(cap(pool.connectionsChan)); pool.counter++ {
		var conn ClientConnector
		if conn, err = NewRPCClient(pool.transport, pool.address, pool.tls,
			pool.keyPath, pool.certPath, pool.caPath, pool.connectAttempts, pool.reconnects,
			pool.connTimeout, pool.replyTimeout, pool.codec,
			pool.internalChan, false); err != nil {
			return
		}
		pool.connectionsChan <- conn
	}
	return
}
