/*
RpcClient for Go RPC Servers
Copyright (C) 2012-2014 ITsysCOM GmbH

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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	//"log/syslog"
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
	//logger                     *syslog.Writer
)

func init() {
	//logger, _ = syslog.New(syslog.LOG_INFO, "RPCClient") // If we need to report anything to syslog
}

// successive Fibonacci numbers.
func Fib() func() time.Duration {
	a, b := 0, 1
	return func() time.Duration {
		a, b = b, a+b
		return time.Duration(a) * time.Second
	}
}

func NewRpcClient(transport, addr string, connectAttempts, reconnects int, connTimeout, replyTimeout time.Duration, codec string, internalConn RpcClientConnection) (*RpcClient, error) {
	var err error
	rpcClient := &RpcClient{transport: transport, address: addr, reconnects: reconnects, connTimeout: connTimeout, replyTimeout: replyTimeout, codec: codec, connection: internalConn, connMux: new(sync.Mutex)}
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
	address      string
	reconnects   int
	connTimeout  time.Duration
	replyTimeout time.Duration
	codec        string // JSON_RPC or GOB_RPC
	connection   RpcClientConnection
	connMux      *sync.Mutex
}

func (self *RpcClient) connect() (err error) {
	self.connMux.Lock()
	defer self.connMux.Unlock()
	if self.codec == INTERNAL_RPC {
		return nil
	} else if self.codec == JSON_HTTP {
		self.connection = &HttpJsonRpcClient{httpClient: new(http.Client), url: self.address}
		return
	}
	// RPC compliant connections here, manually create connection to timeout
	netconn, err := net.DialTimeout(self.transport, self.address, self.connTimeout)
	if err != nil {
		return err
	}
	if self.codec == JSON_RPC {
		self.connection = jsonrpc.NewClient(netconn)
	} else {
		self.connection = rpc.NewClient(netconn)
	}
	if err != nil {
		self.connection = nil // So we don't wrap nil into the interface
	}
	return
}

func (self *RpcClient) reconnect() (err error) {
	if self.codec == JSON_HTTP { // http client has automatic reconnects in place
		return self.connect()
	}
	i := 0
	delay := Fib()
	for {
		if self.reconnects != -1 && i >= self.reconnects { // Maximum reconnects reached, -1 for infinite reconnects
			break
		}
		if err = self.connect(); err == nil { // No error on connect, succcess
			return nil
		}
		i++
		time.Sleep(delay()) // Cound not reconnect, retry
	}
	return errors.New("RECONNECT_FAIL")
}

func (self *RpcClient) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	if args == nil {
		return fmt.Errorf("nil rpc in argument method: %s in: %v out: %v", serviceMethod, args, reply)
	}
	if self.connection == nil {
		err = ErrDisconnected
	} else {
		errChan := make(chan error, 1)
		go func() { errChan <- self.connection.Call(serviceMethod, args, reply) }()
		select {
		case err = <-errChan:
		case <-time.After(self.replyTimeout):
			err = ErrReplyTimeout
		}
	}
	if isNetworkError(err) && self.reconnects != 0 {
		if errReconnect := self.reconnect(); errReconnect != nil {
			return err
		} else { // Run command after reconnect
			return self.connection.Call(serviceMethod, args, reply)
		}
	}
	return err
}

// Connection used in RpcClient, as interface so we can combine the rpc.RpcClient with http one or websocket
type RpcClientConnection interface {
	Call(string, interface{}, interface{}) error
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
	if rcc != nil {
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
		strings.HasPrefix(err.Error(), "rpc: can't find service")
}
