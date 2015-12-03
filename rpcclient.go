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
	"math"
	"math/rand"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	JSON_RPC       = "json"
	JSON_HTTP      = "http_jsonrpc"
	GOB_RPC        = "gob"
	INTERNAL_RPC   = "internal"
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
)

// successive Fibonacci numbers.
func Fib() func() time.Duration {
	a, b := 0, 1
	return func() time.Duration {
		a, b = b, a+b
		return time.Duration(a) * time.Second
	}
}

func NewRpcClient(transport, addr string, connectAttempts, reconnects int, codec string, internalConn RpcClientConnection) (*RpcClient, error) {
	var err error
	rpcClient := &RpcClient{transport: transport, address: addr, reconnects: reconnects, codec: codec, connection: internalConn, connMux: new(sync.Mutex)}
	delay := Fib()
	for i := 0; i < connectAttempts; i++ {
		err = rpcClient.connect()
		if err == nil { //Connected so no need to reiterate
			break
		}
		time.Sleep(delay())
	}
	if err != nil {
		return nil, err
	}
	return rpcClient, nil
}

type RpcClient struct {
	transport  string
	address    string
	reconnects int
	codec      string // JSON_RPC or GOB_RPC
	connection RpcClientConnection
	connMux    *sync.Mutex
}

func (self *RpcClient) connect() (err error) {
	self.connMux.Lock()
	defer self.connMux.Unlock()
	switch self.codec {
	case JSON_RPC:
		self.connection, err = jsonrpc.Dial(self.transport, self.address)
	case JSON_HTTP:
		self.connection = &HttpJsonRpcClient{httpClient: new(http.Client), url: self.address}
	case INTERNAL_RPC:
		return nil // connection should be set on init
	default:
		self.connection, err = rpc.Dial(self.transport, self.address)
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
		if i != -1 && i >= self.reconnects { // Maximum reconnects reached, -1 for infinite reconnects
			break
		}
		if err = self.connect(); err == nil { // No error on connect, succcess
			return nil
		}
		time.Sleep(delay()) // Cound not reconnect, retry
	}
	return errors.New("RECONNECT_FAIL")
}

func (self *RpcClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	err := self.connection.Call(serviceMethod, args, reply)
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
}

func NewRpcClientPool(transmissionType string) *RpcClientPool {
	return &RpcClientPool{transmissionType: transmissionType}
}

func (pool *RpcClientPool) AddClient(rcc RpcClientConnection) {
	pool.connections = append(pool.connections, rcc)
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
		//get first response
		re := <-replyChan
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
	return err != nil && (err == rpc.ErrShutdown ||
		err == ErrReqUnsynchronized)
}
