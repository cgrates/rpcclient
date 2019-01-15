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
	"syscall"
	"time"
)

const (
	JSON_RPC       = "json"
	JSON_HTTP      = "http_jsonrpc"
	GOB_RPC        = "gob"
	INTERNAL_RPC   = "*internal"
	POOL_FIRST     = "first"
	POOL_RANDOM    = "random"
	POOL_NEXT      = "next"
	POOL_BROADCAST = "broadcast"
)

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
	logger                     *syslog.Writer
)

func init() {
	logger, _ = syslog.New(syslog.LOG_INFO, "RPCClient") // If we need to report anything to syslog
}

// successive Fibonacci numbers.
func Fib() func() time.Duration {
	a, b := 0, 1
	return func() time.Duration {
		a, b = b, a+b
		return time.Duration(a*10) * time.Millisecond
	}
}

func NewRpcClient(transport, addr string, tls bool,
	key_path, cert_path, ca_path string, connectAttempts, reconnects int,
	connTimeout, replyTimeout time.Duration, codec string,
	internalConn RpcClientConnection, lazyConnect bool) (rpcClient *RpcClient, err error) {
	if codec != INTERNAL_RPC && codec != JSON_RPC && codec != JSON_HTTP && codec != GOB_RPC {
		return nil, ErrUnsupportedCodec
	}
	if codec == INTERNAL_RPC && reflect.ValueOf(internalConn).IsNil() {
		return nil, ErrInternallyDisconnected
	}
	rpcClient = &RpcClient{transport: transport, tls: tls,
		address: addr, key_path: key_path,
		cert_path: cert_path, ca_path: ca_path, reconnects: reconnects,
		connTimeout: connTimeout, replyTimeout: replyTimeout,
		codec: codec, connection: internalConn}
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
	return rpcClient, err
}

type RpcClient struct {
	transport    string
	tls          bool
	address      string
	key_path     string
	cert_path    string
	ca_path      string
	reconnects   int
	connTimeout  time.Duration
	replyTimeout time.Duration
	codec        string // JSON_RPC or GOB_RPC
	connection   RpcClientConnection
	connMux      sync.RWMutex // protects connection
}

func loadTLSConfig(clientCrt, clientKey, caPath string) (config tls.Config, err error) {
	var cert tls.Certificate
	if clientCrt != "" && clientKey != "" {
		cert, err = tls.LoadX509KeyPair(clientCrt, clientKey)
		if err != nil {
			logger.Crit(fmt.Sprintf("Error: %s when load client cert and key", err))
			return config, err
		}
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		logger.Crit(fmt.Sprintf("Error: %s when load SystemCertPool", err))
		return
	}
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	if caPath != "" {
		ca, err := ioutil.ReadFile(caPath)
		if err != nil {
			logger.Crit(fmt.Sprintf("Error: %s when read CA", err))
			return config, err
		}

		if ok := rootCAs.AppendCertsFromPEM(ca); !ok {
			logger.Crit(fmt.Sprintf("Cannot append certificate authority"))
			return config, err
		}
	}

	config = tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
	}
	return
}

func (self *RpcClient) connect() (err error) {
	self.connMux.Lock()
	defer self.connMux.Unlock()
	if self.codec == INTERNAL_RPC {
		if self.connection == nil {
			return ErrDisconnected
		}
		return
	} else if self.codec == JSON_HTTP {
		if self.tls {
			config, err := loadTLSConfig(self.cert_path, self.key_path, self.ca_path)
			if err != nil {
				return err
			}
			transport := &http.Transport{TLSClientConfig: &config}
			client := &http.Client{Transport: transport}
			self.connection = &HttpJsonRpcClient{httpClient: client, url: self.address}
		} else {
			self.connection = &HttpJsonRpcClient{httpClient: new(http.Client), url: self.address}
		}
		return
	}
	var netconn io.ReadWriteCloser
	if self.tls {
		config, err := loadTLSConfig(self.cert_path, self.key_path, self.ca_path)
		if err != nil {
			return err
		}
		netconn, err = tls.Dial(self.transport, self.address, &config)
		if err != nil {
			logger.Crit(fmt.Sprintf("Error: %s when dialing", err.Error()))
			return err
		}
	} else {
		// RPC compliant connections here, manually create connection to timeout
		netconn, err = net.DialTimeout(self.transport, self.address, self.connTimeout)
		if err != nil {
			self.connection = nil // So we don't wrap nil into the interface
			return err
		}
	}

	if self.codec == JSON_RPC {
		self.connection = jsonrpc.NewClient(netconn)
	} else {
		self.connection = rpc.NewClient(netconn)
	}
	return
}

func (self *RpcClient) isConnected() bool {
	self.connMux.RLock()
	defer self.connMux.RUnlock()
	return self.connection != nil
}

func (self *RpcClient) disconnect() (err error) {
	switch self.codec {
	case INTERNAL_RPC, JSON_HTTP:
	default:
		self.connMux.Lock()
		if self.connection != nil {
			self.connection.(*rpc.Client).Close()
			self.connection = nil
		}
		self.connMux.Unlock()
	}
	return nil
}

func (self *RpcClient) reconnect() (err error) {
	self.disconnect()            // make sure we have cleared the connection so it can be garbage collected
	if self.codec == JSON_HTTP { // http client has automatic reconnects in place
		return self.connect()
	}
	i := 0
	delay := Fib()
	for {
		i++
		if self.reconnects != -1 && i > self.reconnects { // Maximum reconnects reached, -1 for infinite reconnects
			break
		}
		if err = self.connect(); err == nil { // No error on connect, succcess
			return nil
		}
		time.Sleep(delay()) // Cound not reconnect, retry
	}
	return ErrFailedReconnect
}

func (self *RpcClient) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	if args == nil {
		return fmt.Errorf("nil rpc in argument method: %s in: %v out: %v", serviceMethod, args, reply)
	}
	rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface())).Interface() // clone to avoid concurrency
	errChan := make(chan error, 1)
	go func(serviceMethod string, args interface{}, reply interface{}) {
		self.connMux.RLock()
		if self.connection == nil {
			errChan <- ErrDisconnected
		} else {
			if argsClnIface, clnable := args.(RPCCloner); clnable { // try cloning to avoid concurrency
				if argsCloned, err := argsClnIface.RPCClone(); err != nil {
					errChan <- err
					return
				} else {
					args = argsCloned
				}
			}
			errChan <- self.connection.Call(serviceMethod, args, reply)
		}
		self.connMux.RUnlock()
	}(serviceMethod, args, rpl)
	select {
	case err = <-errChan:
	case <-time.After(self.replyTimeout):
		err = ErrReplyTimeout
	}
	if isNetworkError(err) && err != ErrReplyTimeout &&
		err.Error() != ErrSessionNotFound.Error() &&
		self.reconnects != 0 { // ReplyTimeout should not reconnect since it creates loop
		if errReconnect := self.reconnect(); errReconnect != nil {
			return err
		}
		self.connMux.RLock()
		defer self.connMux.RUnlock()
		return self.connection.Call(serviceMethod, args, reply)
	}
	reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(rpl).Elem()) // no errors, copy the reply from clone
	return
}

// Connection used in RpcClient, as interface so we can combine the rpc.RpcClient with http one or websocket
type RpcClientConnection interface {
	Call(string, interface{}, interface{}) error
}

// RPCCloner is an interface for objects to clone parts of themselves which are affected by concurrency at the time of RPC call
type RPCCloner interface {
	RPCClone() (interface{}, error)
}

// Response received for
type JsonRpcResponse struct {
	Id     uint64
	Result *json.RawMessage
	Error  interface{}
}

type HttpJsonRpcClient struct {
	httpClient *http.Client
	id         uint64
	url        string
}

func (self *HttpJsonRpcClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	self.id += 1
	id := self.id
	data, err := json.Marshal(map[string]interface{}{
		"method": serviceMethod,
		"id":     self.id,
		"params": [1]interface{}{args},
	})
	if err != nil {
		return err
	}
	resp, err := self.httpClient.Post(self.url, "application/json", ioutil.NopCloser(strings.NewReader(string(data)))) // Closer so we automatically have close after response
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var jsonRsp JsonRpcResponse
	err = json.Unmarshal(body, &jsonRsp)
	if err != nil {
		return err
	}
	if jsonRsp.Id != id {
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

type RpcClientPool struct {
	transmissionType string
	connections      []RpcClientConnection
	counter          int
	replyTimeout     time.Duration
}

func NewRpcClientPool(transmissionType string, replyTimeout time.Duration) *RpcClientPool {
	return &RpcClientPool{transmissionType: transmissionType, replyTimeout: replyTimeout}
}

func (pool *RpcClientPool) AddClient(rcc RpcClientConnection) {
	if rcc != nil && !reflect.ValueOf(rcc).IsNil() {
		pool.connections = append(pool.connections, rcc)
	}
}

func (pool *RpcClientPool) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	switch pool.transmissionType {
	case POOL_BROADCAST:
		replyChan := make(chan *rpcReplyError, len(pool.connections))
		for _, rc := range pool.connections {
			go func(conn RpcClientConnection) {
				// make a new pointer of the same type
				rpl := reflect.New(reflect.TypeOf(reflect.ValueOf(reply).Elem().Interface()))
				err := conn.Call(serviceMethod, args, rpl.Interface())
				if !isNetworkError(err) {
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
	case POOL_FIRST:
		for _, rc := range pool.connections {
			err = rc.Call(serviceMethod, args, reply)
			if isNetworkError(err) {
				continue
			}
			return
		}
	case POOL_NEXT:
		ln := len(pool.connections)
		rrIndexes := roundIndex(int(math.Mod(float64(pool.counter), float64(ln))), ln)
		pool.counter++
		for _, index := range rrIndexes {
			err = pool.connections[index].Call(serviceMethod, args, reply)
			if isNetworkError(err) {
				continue
			}
			return
		}
	case POOL_RANDOM:
		rand.Seed(time.Now().UnixNano())
		randomIndex := rand.Perm(len(pool.connections))
		for _, index := range randomIndex {
			err = pool.connections[index].Call(serviceMethod, args, reply)
			if isNetworkError(err) {
				continue
			}
			return
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
func roundIndex(start, max int) []int {
	if start < 0 {
		start = 0
	}
	result := make([]int, max)
	for i := 0; i < max; i++ {
		if start+i < max {
			result[i] = start + i
		} else {
			result[i] = int(math.Abs(float64(max - (start + i))))
		}
	}
	return result
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if operr, ok := err.(*net.OpError); ok && strings.HasSuffix(operr.Err.Error(), syscall.ECONNRESET.Error()) { // connection reset
		return true
	}
	return err == rpc.ErrShutdown ||
		err == ErrReqUnsynchronized ||
		err == ErrDisconnected ||
		err == ErrReplyTimeout ||
		err.Error() == ErrSessionNotFound.Error() ||
		strings.HasPrefix(err.Error(), "rpc: can't find service")
}
