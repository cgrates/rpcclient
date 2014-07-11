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
	"errors"
	"fmt"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"
)

var JSON_RPC = "json"
var GOB_RPC = "gob"

func NewRpcClient(transport, addr string, port, reconnects int, codec string) (*RpcClient, error) {
	var rpcClient *RpcClient
	if port == 0 { // Default port used
		rpcClient = &RpcClient{transport: transport, address: addr, reconnects: reconnects, codec: codec}
	} else {
		rpcClient = &RpcClient{transport: transport, address: fmt.Sprintf("%s:%d", addr, port), reconnects: reconnects, codec: codec}
	}
	if err := rpcClient.connect(); err != nil { // No point in configuring if not possible to establish a connection
		return nil, err
	}
	return rpcClient, nil
}

type RpcClient struct {
	transport  string
	address    string
	reconnects int
	codec      string // JSON_RPCrpc or GOB_RPC
	connection *rpc.Client
}

func (self *RpcClient) connect() (err error) {
	if self.codec == JSON_RPC {
		self.connection, err = jsonrpc.Dial(self.transport, self.address)
	} else {
		self.connection, err = rpc.Dial(self.transport, self.address)
	}
	if err != nil {
		return err
	}
	return nil
}

func (self *RpcClient) reconnect() (err error) {
	for i := 0; i < self.reconnects; i++ {
		if self.connection, err = jsonrpc.Dial(self.transport, self.address); err == nil { // No error on connect, success
			return nil
		}
		time.Sleep(time.Duration(i/2) * time.Second) // Cound not reconnect, retry
	}
	return errors.New("RECONNECT_FAIL")
}

func (self *RpcClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	err := self.connection.Call(serviceMethod, args, reply)
	if err != nil && err == rpc.ErrShutdown && self.reconnects != 0 {
		if errReconnect := self.reconnect(); errReconnect != nil {
			return err
		} else { // Run command after reconnect
			return self.connection.Call(serviceMethod, args, reply)
		}
	}
	return err // Original reply otherwise
}
