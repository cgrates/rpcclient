package rpcclient

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/rpc"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"net/rpc/jsonrpc"

	"github.com/cgrates/birpc"
	"github.com/cgrates/birpc/context"
)

type MockRPCClient struct {
	id   string
	c    chan struct{}
	used bool

	cl birpc.ClientConnector
}

func (m *MockRPCClient) Echo(ctx *context.Context, args string, reply *string) error {
	*reply += m.id
	return nil
}

func (m *MockRPCClient) EchoBiRPC(ctx *context.Context, args string, reply *string) error {
	*reply += m.id
	m.cl = ctx.Client
	return nil
}

func (m *MockRPCClient) Handlers() map[string]interface{} {
	return map[string]interface{}{
		"Echo": m.EchoBiRPC,
	}
}

func (m *MockRPCClient) Call(ctx *context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	switch m.id {
	case "offline":
		return ErrReqUnsynchronized
	case "error":
		return errors.New("Not Found")
	case "nerr":
		return birpc.ErrShutdown
	case "callBiRPC":
		return ctx.Client.Call(ctx, "", args, reply)
	case "async":
		m.used = true
		select {
		case <-time.After(20 * time.Millisecond):
			close(m.c)
			return birpc.ErrShutdown
		case <-ctx.Done():
			return ctx.Err()
		}
	case "sleep":
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		fallthrough
	default:
		*reply.(*string) += m.id
		return nil
	}
}

func TestPoolFirst(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolFirst,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "1"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	var response string
	p.Call(context.Background(), "", "", &response)
	if response != "1" {
		t.Error("Error calling client: ", response)
	}
	p = &RPCPool{
		transmissionType: PoolFirst,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "offline"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	p.Call(context.Background(), "", "", &response)
	if response != "12" {
		t.Error("Error calling client: ", response)
	}
}

func TestPoolFirstAsync(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolAsync,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "1"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	var response string
	// we don't verify the response because the connection is asynchronous
	if err := p.Call(context.Background(), "", "", &response); err != nil {
		return
	}

	p = &RPCPool{
		transmissionType: PoolAsync,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "offline"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	if err := p.Call(context.Background(), "", "", &response); err != nil {
		return
	}

}

func TestPoolNext(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolNext,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "1"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	var response string
	p.Call(context.Background(), "", "", &response)
	if response != "1" {
		t.Error("Error calling client: ", response)
	}

	p.Call(context.Background(), "", "", &response)
	if response != "12" {
		t.Error("Error calling client: ", response)
	}

	p = &RPCPool{
		transmissionType: PoolNext,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "nerr"},
			&MockRPCClient{id: "1"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	response = ""
	p.Call(context.Background(), "", "", &response)
	if response != "1" {
		t.Error("Error calling client: ", response)
	}
}

func TestPoolBrodcast(t *testing.T) {
	p := &RPCPool{
		replyTimeout:     time.Second,
		transmissionType: PoolBroadcast,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "1"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	var response string
	if err := p.Call(context.Background(), "", "", &response); err != nil {
		t.Error("Got error: ", err)
	}
	if len(response) != 1 {
		t.Error("Error calling client: ", response)
	}
	p = &RPCPool{
		replyTimeout:     25 * time.Millisecond,
		transmissionType: PoolBroadcast,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "sleep"},
		},
	}
	if err := p.Call(context.Background(), "", "", &response); err != context.DeadlineExceeded {
		t.Errorf("Expected error %s received:%v ", context.DeadlineExceeded, err)
	}
	p = &RPCPool{
		transmissionType: PoolBroadcast,
		replyTimeout:     time.Second,
		connections:      []birpc.ClientConnector{},
	}
	if err := p.Call(context.Background(), "", "", &response); err != ErrDisconnected {
		t.Errorf("Expected error %s received:%v ", ErrDisconnected, err)
	}
}

func TestPoolBrodcastSyncWithError(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolBroadcastSync,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "1"},
			&MockRPCClient{id: "error"},
			&MockRPCClient{id: "error"},
			&MockRPCClient{id: "2"},
		},
	}
	var response string
	if err := p.Call(context.Background(), "", "", &response); err != ErrPartiallyExecuted {
		t.Errorf("Expected error %s received:%v ", ErrPartiallyExecuted, err)
	}
	if len(response) != 1 {
		t.Error("Error calling client: ", response)
	}

	p = &RPCPool{
		transmissionType: PoolBroadcastSync,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "error"},
			&MockRPCClient{id: "error"},
			&MockRPCClient{id: "error"},
		},
	}
	var response2 string
	if err := p.Call(context.Background(), "", "", &response2); err != ErrPartiallyExecuted {
		t.Errorf("Expected error %s received:%v ", ErrPartiallyExecuted, err)
	}
	if len(response2) != 0 {
		t.Error("Error calling client: ", response2)
	}

	p = &RPCPool{
		transmissionType: PoolBroadcastSync,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "nerr"},
			&MockRPCClient{id: "nerr"},
			&MockRPCClient{id: "nerr"},
		},
	}
	if err := p.Call(context.Background(), "", "", &response2); err != birpc.ErrShutdown {
		t.Errorf("Expected error %s received:%v ", birpc.ErrShutdown, err)
	}
	if len(response2) != 0 {
		t.Error("Error calling client: ", response2)
	}

}

func TestPoolBrodcastSyncWithoutError(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolBroadcastSync,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "1"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
		},
	}
	var response string
	if err := p.Call(context.Background(), "", "", &response); err != nil {
		t.Error("Got error: ", err)
	}
	if len(response) != 1 {
		t.Error("Error calling client: ", response)
	}
}

func TestPoolRANDOM(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolRandom,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "nerr"},
			&MockRPCClient{id: "1"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	m := make(map[string]struct{}, 4)
	for i := 0; i < 100; i++ {
		var response string
		if err := p.Call(context.Background(), "", "", &response); err != nil {
			t.Error("Got error: ", err)
		}
		m[response] = struct{}{}
	}
	if len(m) < 4 { // should use them all
		t.Error("Error calling client: ", m)
	}
}

func TestWrongPool(t *testing.T) { // in case of a unknow pool we do nothing at call
	p := &RPCPool{
		transmissionType: "Not a supported pool",
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "1"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	var response string
	if err := p.Call(context.Background(), "", "", &response); err != nil {
		t.Error("Got error: ", err)
	}
	if len(response) != 0 {
		t.Error("Error calling client: ", response)
	}
}

func TestPoolFirstPositive(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolFirstPositive,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "error"},
			&MockRPCClient{id: "error"},
			&MockRPCClient{id: "3"},
			&MockRPCClient{id: "4"},
		},
	}
	var response string
	p.Call(context.Background(), "", "", &response)
	if response != "3" {
		t.Error("Error calling client: ", response)
	}

	p = &RPCPool{
		transmissionType: PoolFirstPositive,
		connections: []birpc.ClientConnector{
			&MockRPCClient{id: "error"},
			&MockRPCClient{id: "2"},
			&MockRPCClient{id: "error"},
			&MockRPCClient{id: "4"},
		},
	}
	response = ""
	p.Call(context.Background(), "", "", &response)
	if response != "2" {
		t.Error("Error calling client: ", response)
	}
}

func TestNewRpcParallelClientPool(t *testing.T) {
	internalChan := make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "1"}
	rpcppool, err := NewRPCParallelClientPool(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 2, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	var reply string
	if err = rpcppool.Call(context.Background(), "", "", &reply); err != nil {
		t.Error(err)
	} else if reply != "1" {
		t.Errorf("Expected: \"1\" received: %q", reply)
	} else if rpcppool.counter != 1 {
		t.Errorf("Expected: the counter to be 1 received: %v", rpcppool.counter)
	} else if len(rpcppool.connectionsChan) != 1 {
		t.Errorf("Expected: 1 received: %v", len(rpcppool.connectionsChan))
	}
	reply = ""
	if err = rpcppool.Call(context.Background(), "", "", &reply); err != nil { // this should take the connection from earlier
		t.Error(err)
	} else if reply != "1" {
		t.Errorf("Expected: \"1\" received: %q", reply)
	} else if rpcppool.counter != 1 {
		t.Errorf("Expected: the counter to be 1 received: %v", rpcppool.counter)
	} else if len(rpcppool.connectionsChan) != 1 {
		t.Errorf("Expected: 1 received: %v", len(rpcppool.connectionsChan))
	}
	rpcppool, err = NewRPCParallelClientPool(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 2, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	rpcppool.codec = "Not a supported codec"
	if err = rpcppool.Call(context.Background(), "", "", &reply); err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}

	_, err = NewRPCParallelClientPool(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, "Not a supported codec", internalChan, 2, false, nil)
	if err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}

	_, err = NewRPCParallelClientPool(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, InternalRPC, nil, 2, false, nil)
	if err != ErrInternallyDisconnected {
		t.Errorf("Expected error: %s received: %v", ErrInternallyDisconnected, err)
	}

	close(internalChan)
	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "sleep"}
	rpcppool, err = NewRPCParallelClientPool(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 70*time.Millisecond, InternalRPC, internalChan, 2, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	parlallel := func(t *testing.T) {
		reply := ""
		if err = rpcppool.Call(context.Background(), "", "", &reply); err != nil { // this should take the connection from earlier
			t.Error(err)
		} else if reply != "sleep" {
			t.Errorf("Expected: \"sleep\" received: %q", reply)
		}
		group.Done()
	}
	for i := 0; i < 4; i++ {
		group.Add(1)
		go parlallel(t)
	}
	group.Wait()

	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "1"}
	rpcppool, err = NewRPCParallelClientPool(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 3, true, nil)
	if err != nil {
		t.Fatal(err)
	} else if rpcppool.counter != 3 {
		t.Errorf("Expected: the counter to be 3 received: %v", rpcppool.counter)
	} else if len(rpcppool.connectionsChan) != 3 {
		t.Errorf("Expected: 1 received: %v", len(rpcppool.connectionsChan))
	}

	rpcppool, err = NewRPCParallelClientPool(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 3, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	rpcppool.codec = "Not a supported codec"
	if err = rpcppool.initConns(context.Background()); err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}
}

func TestFailoverAndRetryConditions(t *testing.T) {
	retry := func(err error) bool {
		return isNetworkErr(err) || isServiceErr(err)
	}
	failover := ShouldFailover
	tests := []struct {
		name     string
		err      error
		passCond func(error) bool
		want     bool
	}{
		{
			name: "nil should not trigger failover/retry",
			err:  nil,
			passCond: func(err error) bool {
				return !failover(err) && !retry(err)
			},
		},
		{
			name: "NOT_FOUND should not trigger failover/retry",
			err:  errors.New("NOT_FOUND"),
			passCond: func(err error) bool {
				return !failover(err) && !retry(err)
			},
		},
		{
			name: "failover & retry on syscall.ECONNRESET (network err)",
			err:  &net.OpError{Err: syscall.ECONNRESET},
			passCond: func(err error) bool {

				return failover(err) && retry(err)
			},
		},
		{
			name: "failover & retry on DISCONNECTED (network err)",
			err:  ErrDisconnected,
			passCond: func(err error) bool {
				return failover(err) && retry(err)
			},
		},
		{
			name: "failover & retry on DNS errors",
			err:  new(net.DNSError),
			passCond: func(err error) bool {
				return failover(err) && retry(err)
			},
		},
		{
			name: "failover & retry on 'can't find service' errors",
			err:  errors.New("rpc: can't find service TestObj.Nothing"),
			passCond: func(err error) bool {
				return failover(err) && retry(err)
			},
		},
		{
			name: "failover & retry on 'connection is shut down' errors",
			err:  birpc.ErrShutdown,
			passCond: func(err error) bool {
				return failover(err) && retry(err)
			},
		},
		{
			name: "failover & retry on REQ_UNSYNCHRONIZED (sentinel)",
			err:  ErrReqUnsynchronized,
			passCond: func(err error) bool {
				return failover(err) && retry(err)
			},
		},
		{
			name: "failover & retry on REQ_UNSYNCHRONIZED (string)",
			err:  errors.New(ErrReqUnsynchronized.Error()),
			passCond: func(err error) bool {
				return failover(err) && retry(err)
			},
		},
		{
			name: "timeout errors should trigger failover but not retry (sentinel)",
			err:  context.DeadlineExceeded,
			passCond: func(err error) bool {
				return failover(err) && !retry(err)
			},
		},
		{
			name: "timeout errors should trigger failover but not retry (string)",
			err:  errors.New(context.DeadlineExceeded.Error()),
			passCond: func(err error) bool {
				return failover(err) && !retry(err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.passCond(tt.err) {
				t.Errorf("unexpected behavour: retry %v, failover %v on error %v",
					retry(tt.err), failover(tt.err), tt.err)
			}
		})
	}
}

func TestRoundIndex(t *testing.T) {
	exp := []int{0, 1, 2, 3, 4, 5}
	if reply := roundIndex(-10, 6); !reflect.DeepEqual(exp, reply) {
		t.Errorf("Expected: %v received: %v", exp, reply)
	}

	exp = []int{3, 4, 5, 0, 1, 2}
	if reply := roundIndex(3, 6); !reflect.DeepEqual(exp, reply) {
		t.Errorf("Expected: %v received: %v", exp, reply)
	}
}

func TestNewRPCPool(t *testing.T) {
	exp := &RPCPool{
		transmissionType: PoolBroadcast,
		replyTimeout:     time.Millisecond,
	}

	pool := NewRPCPool(PoolBroadcast, time.Millisecond)
	if !reflect.DeepEqual(exp, pool) {
		t.Errorf("Expected: %v received: %v", exp, pool)
	}
	internalChan := make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "1"}
	client, err := NewRPCParallelClientPool(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 2, false, nil)
	if err != nil {
		t.Error(err)
	}
	if len(pool.connections) != 0 {
		t.Errorf("Expected: %v received: %v", 0, len(pool.connections))
	}
	pool.AddClient(client)
	if len(pool.connections) != 1 {
		t.Errorf("Expected: %v received: %v", 1, len(pool.connections))
	}
}

func TestFib(t *testing.T) {
	fib := fibDuration(10*time.Millisecond, 0)
	if tmp := fib(); tmp != 10*time.Millisecond {
		t.Errorf("Expecting: %s, received %s", 10*time.Millisecond, tmp)
	}
	if tmp := fib(); tmp != 10*time.Millisecond {
		t.Errorf("Expecting: %s, received %s", 10*time.Millisecond, tmp)
	}
	if tmp := fib(); tmp != 20*time.Millisecond {
		t.Errorf("Expecting: %s, received %s", 20*time.Millisecond, tmp)
	}
	if tmp := fib(); tmp != 30*time.Millisecond {
		t.Errorf("Expecting: %s, received %s", 30*time.Millisecond, tmp)
	}
	if tmp := fib(); tmp != 50*time.Millisecond {
		t.Errorf("Expecting: %s, received %s", 50*time.Millisecond, tmp)
	}
}

func TestNewRPCClient(t *testing.T) {
	if _, err := NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, "Not a supported codec", nil, true, nil); err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}
	if _, err := NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, InternalRPC, nil, true, nil); err != ErrInternallyDisconnected {
		t.Errorf("Expected error: %s received: %v", ErrInternallyDisconnected, err)
	}

	internalChan := make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "1"}
	exp := &RPCClient{
		transport:    "transport",
		tls:          false,
		address:      "addr",
		keyPath:      "",
		certPath:     "",
		caPath:       "",
		reconnects:   10,
		connTimeout:  time.Millisecond,
		replyTimeout: 50 * time.Millisecond,
		codec:        JSONrpc,
	}
	client, err := NewRPCClient(context.Background(), "transport", "addr", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, JSONrpc, nil, true, nil)
	if err != nil {
		t.Error(err)
	}
	// no reflect because of the Mutex
	if exp.transport != client.transport {
		t.Errorf("Expected: %v received: %v", exp.transport, client.transport)
	}
	if exp.tls != client.tls {
		t.Errorf("Expected: %v received: %v", exp.tls, client.tls)
	}
	if exp.address != client.address {
		t.Errorf("Expected: %v received: %v", exp.address, client.address)
	}
	if exp.keyPath != client.keyPath {
		t.Errorf("Expected: %v received: %v", exp.keyPath, client.keyPath)
	}
	if exp.certPath != client.certPath {
		t.Errorf("Expected: %v received: %v", exp.certPath, client.certPath)
	}
	if exp.caPath != client.caPath {
		t.Errorf("Expected: %v received: %v", exp.caPath, client.caPath)
	}
	if exp.reconnects != client.reconnects {
		t.Errorf("Expected: %v received: %v", exp.reconnects, client.reconnects)
	}
	if exp.connTimeout != client.connTimeout {
		t.Errorf("Expected: %v received: %v", exp.connTimeout, client.connTimeout)
	}
	if exp.replyTimeout != client.replyTimeout {
		t.Errorf("Expected: %v received: %v", exp.replyTimeout, client.replyTimeout)
	}
	if exp.codec != client.codec {
		t.Errorf("Expected: %v received: %v", exp.codec, client.codec)
	}
	if client.isConnected() {
		t.Errorf("Expected to not start the connection if lazzyConnect is on true")
	}
	_, err = NewRPCClient(context.Background(), "transport", "addr", false, "", "", "", 5, 10,
		0, fibDuration, 5*time.Millisecond, 5*time.Millisecond, JSONrpc, nil, false, nil)
	if err == nil {
		t.Errorf("Expected connection error received:%v", err)
	}

	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- nil
	if _, err := NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, false, nil); err != ErrDisconnected {
		t.Errorf("Expected error: %s received: %v", ErrDisconnected, err)
	}
	internalChan = make(chan birpc.ClientConnector, 1)
	if _, err := NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, time.Millisecond, InternalRPC, internalChan, false, nil); err != context.DeadlineExceeded {
		t.Errorf("Expected error: %s received: %v", context.DeadlineExceeded, err)
	}
	if client, err := NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, time.Millisecond, HTTPjson, internalChan, false, nil); err != nil {
		t.Errorf("Expected to create the http connection received error: %v", err)
	} else if err = client.reconnect(context.Background()); err != nil {
		t.Error(err)
	} else if !client.isConnected() {
		t.Errorf("Expected the connection to be started")
	}

	// mock the server
	addr := "localhost:0"
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	addr = l.Addr().String()
	client, err = NewRPCClient(context.Background(), "tcp", addr, false, "", "", "", 5, 10,
		0, fibDuration, 5*time.Millisecond, 5*time.Millisecond, JSONrpc, nil, false, nil)
	if err != nil {
		t.Error(err)
	} else if !client.isConnected() {
		t.Errorf("Expected to start the connection")
	}
	client.disconnect()
	if client.isConnected() {
		t.Errorf("Expected to stop the connection after disconnect")
	}

	client, err = NewRPCClient(context.Background(), "tcp", addr, false, "", "", "", 5, 10,
		0, fibDuration, 5*time.Millisecond, 5*time.Millisecond, BiRPCJSON, nil, false, new(MockRPCClient))
	if err != nil {
		t.Error(err)
	} else if !client.isConnected() {
		t.Errorf("Expected to start the connection")
	}
	client.disconnect()
	if client.isConnected() {
		t.Errorf("Expected to stop the connection after disconnect")
	}

	client, err = NewRPCClient(context.Background(), "tcp", addr, false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, time.Millisecond, GOBrpc, nil, false, nil)
	if err != nil {
		t.Error(err)
	} else if !client.isConnected() {
		t.Errorf("Expected to start the connection")
	}
	if err = client.reconnect(context.Background()); err != nil {
		t.Error(err)
	} else if !client.isConnected() {
		t.Errorf("Expected to restart the connection")
	}
	client, err = NewRPCClient(context.Background(), "tcp", addr, false, "", "", "", 5, 10,
		0, fibDuration, 5*time.Millisecond, 5*time.Millisecond, BiRPCGOB, nil, false, new(MockRPCClient))
	if err != nil {
		t.Error(err)
	} else if !client.isConnected() {
		t.Errorf("Expected to start the connection")
	}
	client.disconnect()
	if client.isConnected() {
		t.Errorf("Expected to stop the connection after disconnect")
	}
	l.Close()
	if err = client.reconnect(context.Background()); err != ErrFailedReconnect {
		t.Errorf("Expected error: %s received: %v", ErrFailedReconnect, err)
	}
	if client.isConnected() {
		t.Errorf("Expected to stop the connection after error on reconnect")
	}
}

type cloner int

func (c cloner) RPCClone() (interface{}, error) {
	if int(c) == -1 {
		return nil, fmt.Errorf("Not cloneable")
	}
	return c, nil
}
func TestRPCClientCall(t *testing.T) {
	internalChan := make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "1"}
	client, err := NewRPCClient(context.Background(), "transport", "addr", false, "", "", "", 5, 10,
		0, fibDuration, 10*time.Millisecond, 5*time.Millisecond, InternalRPC, internalChan, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	experrMsg := fmt.Sprintf("nil rpc in argument method: %s in: %v out: %v", "", nil, nil)
	if err = client.Call(context.Background(), "", nil, nil); err == nil || err.Error() != experrMsg {
		t.Errorf("Expected error: %s received: %v", experrMsg, err)
	}

	client.connection = nil
	var reply *string
	experrMsg = fmt.Sprintf("nil rpc in argument method: %s in: %v out: %v", "", "", nil)
	if err = client.Call(context.Background(), "", "", reply); err == nil || err.Error() != experrMsg {
		t.Errorf("Expected error: %s received: %v", experrMsg, err)
	}
	reply = new(string)
	if err = client.Call(context.Background(), "", "", reply); err != nil {
		t.Error(err)
	}

	experrMsg = "Not cloneable"
	if err = client.Call(context.Background(), "", cloner(0), reply); err != nil {
		t.Error(err)
	}

	if err = client.Call(context.Background(), "", cloner(-1), reply); err == nil || err.Error() != experrMsg {
		t.Errorf("Expected error: %s received: %v", experrMsg, err)
	}

	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- nil
	client, err = NewRPCClient(context.Background(), "transport", "addr", false, "", "", "", 1, 1,
		0, fibDuration, 10*time.Millisecond, 5*time.Millisecond, InternalRPC, internalChan, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	client.connection = nil
	if err = client.Call(context.Background(), "", "", reply); err != ErrDisconnected {
		t.Errorf("Expected error: %s received: %v", ErrDisconnected, err)
	}
	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "sleep"}
	client, err = NewRPCClient(context.Background(), "transport", "addr", false, "", "", "", 1, 1,
		0, fibDuration, 10*time.Millisecond, 5*time.Millisecond, InternalRPC, internalChan, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Call(context.Background(), "", "", reply); err != context.DeadlineExceeded {
		t.Errorf("Expected error: %s received: %v", context.DeadlineExceeded, err)
	}
}

func TestRPCClientCallTimeout(t *testing.T) {
	internalChan := make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "sleep"}
	cl, err := NewRPCClient(context.Background(), "", "", false, "", "", "", 1, 1,
		0, fibDuration, time.Millisecond, 25*time.Millisecond, InternalRPC, internalChan, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	var reply string
	if err = cl.Call(context.Background(), "", "", &reply); err != context.DeadlineExceeded {
		t.Errorf("Expected error: %s received: %v", context.DeadlineExceeded, err)
	}
}

func TestRPCPoolBroadcastAsync(t *testing.T) {
	c := []*MockRPCClient{
		{id: "async", c: make(chan struct{}, 1)},
		{id: "async", c: make(chan struct{}, 1)},
		{id: "async", c: make(chan struct{}, 1)},
		{id: "async", c: make(chan struct{}, 1)},
	}
	p := &RPCPool{
		transmissionType: PoolBroadcastAsync,
	}
	for _, i := range c {
		p.connections = append(p.connections, i)
	}
	d := make(chan struct{}, 1)
	go func() {
		var response string
		defer close(d)
		if err := p.Call(context.Background(), "", "", &response); err != nil {
			t.Error(err)
			return
		}
		if response != "" {
			t.Error("Error calling client: ", response)
		}
	}()
	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Timeout")
	case <-d:
	}
	for _, i := range c {
		runtime.Gosched()
		<-i.c // wait for the call goroutine to end
		if !i.used {
			t.Fatalf("Expected all connection to be called")
		}
	}

}

func TestRPCClientInternalConnect(t *testing.T) {
	internalChan := make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "1"}
	rpcc, err := NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = rpcc.connect(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRPCClientBiRPCInternalConnect(t *testing.T) {
	internalChan := make(chan birpc.ClientConnector, 1)
	server := &MockRPCClient{id: "callBiRPC"}
	internalChan <- server
	birpcClient := &MockRPCClient{id: "2"}
	p, err := NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, BiRPCInternal, internalChan, false, birpcClient)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	var response string
	ctx := context.Background()
	ctx.Client = birpcClient
	p.Call(ctx, "", "", &response)
	if response != "2" {
		t.Error("Error calling client: ", response)
	}
	close(internalChan)

	internalChan = make(chan birpc.ClientConnector, 1)
	_, err = NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 0, BiRPCInternal, internalChan, false, &MockRPCClient{id: "2"})
	if err != context.DeadlineExceeded {
		t.Errorf("Expected error %s received:%v ", context.DeadlineExceeded, err)
	}

	internalChan <- nil
	_, err = NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 50*time.Millisecond, BiRPCInternal, internalChan, false, &MockRPCClient{id: "2"})
	if err != ErrDisconnected {
		t.Errorf("Expected error %s received:%v ", ErrDisconnected, err)
	}

	close(internalChan)

	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- &RPCParallelClientPool{}

	_, err = NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 0, BiRPCJSON, internalChan, false, nil)
	if err != ErrUnsupportedBiRPC {
		t.Errorf("Expected error %s received:%v ", ErrUnsupportedBiRPC, err)
	}

	_, err = NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 0, BiRPCInternal, internalChan, false, nil)
	if err != ErrUnsupportedBiRPC {
		t.Errorf("Expected error %s received:%v ", ErrUnsupportedBiRPC, err)
	}

	_, err = NewRPCClient(context.Background(), "", "", false, "", "", "", 5, 10,
		0, fibDuration, time.Millisecond, 0, BiRPCInternal, nil, false, nil)
	if err != ErrInternallyDisconnected {
		t.Errorf("Expected error %s received:%v ", ErrInternallyDisconnected, err)
	}
	close(internalChan)
}

func TestRPCClientnewNetConnDialError(t *testing.T) {
	client := &RPCClient{
		tls:     true,
		address: "addr",
	}

	experr := "dial: unknown network "
	_, err := client.newNetConn()

	if err == nil || err.Error() != experr {
		t.Fatalf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientnewNetConnLoadTLSConfigError(t *testing.T) {
	client := &RPCClient{
		tls:      true,
		address:  "addr",
		certPath: "invalidCertPath",
		keyPath:  "invalidKeyPath",
		caPath:   "invalidCaPath",
	}

	experr := "open invalidCertPath: no such file or directory"
	_, err := client.newNetConn()

	if err == nil || err.Error() != experr {
		t.Fatalf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientconnectTLSTrue(t *testing.T) {
	client := &RPCClient{
		tls:   true,
		codec: HTTPjson,
	}

	err := client.connect(context.Background())

	if err != nil {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", nil, err)
	}
}
func TestRPCClientconnectTLSFalse(t *testing.T) {
	client := &RPCClient{
		tls:   false,
		codec: HTTPjson,
	}

	err := client.connect(context.Background())

	if err != nil {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", nil, err)
	}
}

func TestRPCClientconnectInvalidPath(t *testing.T) {
	client := &RPCClient{
		tls:    true,
		codec:  HTTPjson,
		caPath: "invalid",
	}

	experr := "open invalid: no such file or directory"
	err := client.connect(context.Background())

	if err == nil || err.Error() != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientHTTPjsonCallPostFail(t *testing.T) {
	client := &HTTPjsonRPCClient{
		httpClient: http.DefaultClient,
	}
	serviceMethod := ""
	var args interface{}
	var reply interface{}

	experr := "Post \"\": unsupported protocol scheme \"\""
	err := client.Call(context.TODO(), serviceMethod, args, reply)

	if err == nil || err.Error() != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientHTTPjsonCallInvalidJSON(t *testing.T) {
	client := &HTTPjsonRPCClient{
		httpClient: http.DefaultClient,
	}
	serviceMethod := ""
	var args interface{} = make(chan int)
	var reply interface{}

	experr := "json: unsupported type: chan int"
	err := client.Call(context.TODO(), serviceMethod, args, reply)

	if err == nil || err.Error() != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientHTTPjsonCallDecodeFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {

	}))
	client := &HTTPjsonRPCClient{
		httpClient: http.DefaultClient,
		url:        srv.URL,
	}
	serviceMethod := ""
	var args interface{}
	var reply interface{}

	experr := io.EOF
	err := client.Call(context.TODO(), serviceMethod, args, reply)

	if err == nil || err != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientHTTPjsonCallUnsynchronized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{\"valid\":\"json\"}"))
	}))
	client := &HTTPjsonRPCClient{
		httpClient: http.DefaultClient,
		url:        srv.URL,
	}
	serviceMethod := ""
	args := ""
	var reply *string

	experr := ErrReqUnsynchronized
	err := client.Call(context.TODO(), serviceMethod, args, reply)

	if err == nil || err != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientHTTPjsonCallInvalidError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{\"id\":1,\"method\":\"method\",\"params\":\"args\"}"))
	}))
	client := &HTTPjsonRPCClient{
		httpClient: http.DefaultClient,
		url:        srv.URL,
	}
	serviceMethod := ""
	args := ""
	var reply *string

	experr := fmt.Sprintf("invalid error %v", nil)
	err := client.Call(context.TODO(), serviceMethod, args, reply)

	if err == nil || err.Error() != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientHTTPjsonCallSpecifiedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{\"ID\":1,\"Error\":\"specified error\",\"Result\":\"result\"}"))
	}))
	client := &HTTPjsonRPCClient{
		httpClient: http.DefaultClient,
		url:        srv.URL,
	}
	serviceMethod := ""
	args := ""
	var reply *string

	experr := "specified error"
	err := client.Call(context.TODO(), serviceMethod, args, reply)

	if err == nil || err.Error() != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientHTTPjsonCallUnspecifiedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{\"ID\":1,\"Error\":\"\",\"Result\":\"result\"}"))
	}))
	client := &HTTPjsonRPCClient{
		httpClient: http.DefaultClient,
		url:        srv.URL,
	}
	serviceMethod := ""
	args := ""
	var reply *string

	experr := "unspecified error"
	err := client.Call(context.TODO(), serviceMethod, args, reply)

	if err == nil || err.Error() != experr {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", experr, err)
	}
}

func TestRPCClientHTTPjsonCallSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{\"ID\":1,\"Result\":\"5\"}"))
	}))
	client := &HTTPjsonRPCClient{
		httpClient: http.DefaultClient,
		url:        srv.URL,
	}
	serviceMethod := ""
	args := ""
	var reply string

	err := client.Call(context.TODO(), serviceMethod, args, &reply)

	if err != nil {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", nil, err)
	}
}

func fibDuration(durationUnit, maxDuration time.Duration) func() time.Duration {
	a, b := 0, 1
	return func() time.Duration {
		a, b = b, a+b
		fibNrAsDuration := time.Duration(a) * durationUnit
		if maxDuration > 0 && maxDuration < fibNrAsDuration {
			return maxDuration
		}
		return fibNrAsDuration
	}
}

func TestStressRPCClient(t *testing.T) {
	nrOfGoroutines := 10 // nr. of goroutines to spawn
	testCases := []struct {
		codec         string
		serviceMethod string
	}{
		{
			codec:         InternalRPC,
			serviceMethod: "testObj.Add",
		},
		{
			codec:         JSONrpc,
			serviceMethod: "testObj.Add",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("RPCClient Stress Test For Codec %s", tc.codec), func(t *testing.T) {

			// RPCClient setup
			var intChan chan context.ClientConnector
			var ln net.Listener
			var err error
			switch tc.codec {
			case InternalRPC:
				intChan = make(chan context.ClientConnector, 1)
				intChan <- &TestObj{}
			case JSONrpc:
				ln = serve(t, "tcp", ":0", JSONrpc, new(TestObj))
			}

			var network, address string
			if ln != nil {
				network = ln.Addr().Network()
				address = ln.Addr().String()
			}

			rpcClient, err := NewRPCClient(context.Background(), network, address,
				false, "", "", "", 5, 5, 0, fibDuration, 2*time.Second, 2*time.Second, tc.codec,
				intChan, false, nil)
			if err != nil {
				t.Fatal(err)
			}

			var wg sync.WaitGroup
			wg.Add(nrOfGoroutines)
			var disconnects, reconnects int64
			var connChecks, connected, disconnected int64
			var totalCalls, failedCalls, failedLogic, successfulCalls int64

			for i := 0; i < nrOfGoroutines; i++ {
				go func() {
					defer wg.Done()
					cases := rand.Intn(10)
					switch cases {
					// disconnect case
					case 1:
						atomic.AddInt64(&disconnects, 1)
						rpcClient.disconnect()
					// reconnect case
					case 2:
						atomic.AddInt64(&reconnects, 1)
						ctxTO, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						rpcClient.reconnect(ctxTO)
						cancel()
					// connection check case
					case 3:
						atomic.AddInt64(&connChecks, 1)
						ok := rpcClient.isConnected()
						if ok {
							atomic.AddInt64(&connected, 1)
							return
						}
						atomic.AddInt64(&disconnected, 1)
					// normal call
					default:
						var reply int
						args := &TestArgs{
							A: rand.Intn(50),
							B: rand.Intn(50),
						}

						atomic.AddInt64(&totalCalls, 1)
						err := rpcClient.Call(context.Background(), tc.serviceMethod, args, &reply)
						if err != nil {
							atomic.AddInt64(&failedCalls, 1)
							return
						}
						if reply != args.A+args.B {
							atomic.AddInt64(&failedLogic, 1)
							return
						}
						atomic.AddInt64(&successfulCalls, 1)
					}

				}()
			}

			wg.Wait()
			// fmt.Println("disconnects:", disconnects)
			// fmt.Println("reconnects:", reconnects)
			// fmt.Println("connChecks:", connChecks)
			// fmt.Println("connected:", connected)
			// fmt.Println("disconnected:", disconnected)
			// fmt.Println("totalCalls:", totalCalls)
			// fmt.Println("failedCalls:", failedCalls)
			// fmt.Println("failedLogic:", failedLogic)
			// fmt.Println("successfulCalls:", successfulCalls)
			// fmt.Println("==========================")
		})
	}

}

func serve(tb testing.TB, network, address string, codec string, rcvrs ...birpc.ClientConnector) net.Listener {
	tb.Helper()
	server := rpc.NewServer()
	for _, rcvr := range rcvrs {
		if err := server.Register(rcvr); err != nil {
			tb.Fatal(err)
		}
	}
	l, err := net.Listen(network, address)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			switch codec {
			case JSONrpc:
				go server.ServeCodec(jsonrpc.NewServerCodec(conn))
			case GOBrpc:
				go server.ServeConn(conn)
			}
		}
	}()
	return l
}

type TestArgs struct {
	A, B int
}

type TestObj struct {
	returnErr bool
}

func (t *TestObj) Add(in *TestArgs, out *int) error {
	if in == nil {
		return errors.New("nil args")
	}
	*out = in.A + in.B
	return nil
}

func (t *TestObj) ReturnErr(err string, _ *string) error {
	if !t.returnErr || err == "" {
		return nil
	}
	switch err {
	case "":
		return nil
	case context.DeadlineExceeded.Error():
		return context.DeadlineExceeded
	case context.Canceled.Error():
		return context.Canceled
	default:
		return errors.New(err)
	}
}

func (t *TestObj) Call(ctx *context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	var unsupportedService error = errors.New("unsupported service method")
	var serverError error = errors.New("serverError")
	splitMethod := strings.Split(serviceMethod, ".")
	if len(splitMethod) != 2 {
		return unsupportedService
	}
	// get method
	method := reflect.ValueOf(t).MethodByName(splitMethod[1])
	if !method.IsValid() {
		return unsupportedService
	}

	// construct the params
	params := []reflect.Value{reflect.ValueOf(args), reflect.ValueOf(reply)}

	ret := method.Call(params)
	if len(ret) != 1 {
		return serverError
	}
	if ret[0].Interface() == nil {
		return nil
	}
	err, ok := ret[0].Interface().(error)
	if !ok {
		return serverError
	}
	return err
}

// TestRPCClientFailover verifies that failover occurs when connection errors or
// context.DeadlineExceeded errors happen for *first type RPCClient.
func TestRPCClientFailover(t *testing.T) {
	listeners := make([]net.Listener, 0, 3)
	listeners = append(listeners, serve(t, "tcp", ":0", JSONrpc, new(TestObj)))
	listeners = append(listeners, serve(t, "tcp", ":0", JSONrpc, &TestObj{returnErr: true}))
	listeners = append(listeners, serve(t, "tcp", ":0", JSONrpc, new(TestObj)))

	pool := NewRPCPool(PoolFirst, 0)
	for _, ln := range listeners {
		rpcClient, err := NewRPCClient(context.Background(), "tcp", ln.Addr().String(),
			false, "", "", "", 1, 0, 0, fibDuration, 2*time.Second, 2*time.Second, JSONrpc,
			nil, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		pool.AddClient(rpcClient)
	}

	// Simulate a "connection refused" error.
	listeners[0].Close() // close the first listener to avoid leaks

	// Close the connection manually since the ServeCodec goroutine is still running after closing the listener.
	if err := pool.connections[0].(*RPCClient).connection.(io.Closer).Close(); err != nil {
		t.Error(err)
	}

	// The first RPCClient.Call will fail to connect, triggering a failover to the next RPCClient.
	// The second RPCClient.Call will timeout, triggering a failover to the third RPCClient, which should succeed.
	errArg := context.DeadlineExceeded.Error()
	var reply string
	if err := pool.Call(context.Background(), "TestObj.ReturnErr", errArg, &reply); err != nil {
		t.Error(err)
	}
}

var prl = flag.Int("prl", 0, "parallelism")

func BenchmarkRPCPool(b *testing.B) {
	benchmarks := []struct {
		codec    string
		strategy string
	}{
		{
			codec:    InternalRPC,
			strategy: PoolFirst,
		},
		{
			codec:    InternalRPC,
			strategy: PoolFirstPositive,
		},
		{
			codec:    InternalRPC,
			strategy: PoolFirstPositiveAsync,
		},
		{
			codec:    InternalRPC,
			strategy: PoolNext,
		},
		{
			codec:    InternalRPC,
			strategy: PoolRandom,
		},
		{
			codec:    InternalRPC,
			strategy: PoolBroadcast,
		},
		{
			codec:    InternalRPC,
			strategy: PoolBroadcastSync,
		},
		{
			codec:    InternalRPC,
			strategy: PoolParallel,
		},
		{
			codec:    JSONrpc,
			strategy: PoolFirst,
		},
		{
			codec:    JSONrpc,
			strategy: PoolFirstPositive,
		},
		{
			codec:    JSONrpc,
			strategy: PoolFirstPositiveAsync,
		},
		{
			codec:    JSONrpc,
			strategy: PoolNext,
		},
		{
			codec:    JSONrpc,
			strategy: PoolRandom,
		},
		{
			codec:    JSONrpc,
			strategy: PoolBroadcast,
		},
		{
			codec:    JSONrpc,
			strategy: PoolBroadcastSync,
		},
		{
			codec:    JSONrpc,
			strategy: PoolParallel,
		},
		{
			codec:    GOBrpc,
			strategy: PoolFirst,
		},
		{
			codec:    GOBrpc,
			strategy: PoolFirstPositive,
		},
		{
			codec:    GOBrpc,
			strategy: PoolFirstPositiveAsync,
		},
		{
			codec:    GOBrpc,
			strategy: PoolNext,
		},
		{
			codec:    GOBrpc,
			strategy: PoolRandom,
		},
		{
			codec:    GOBrpc,
			strategy: PoolBroadcast,
		},
		{
			codec:    GOBrpc,
			strategy: PoolBroadcastSync,
		},
		{
			codec:    GOBrpc,
			strategy: PoolParallel,
		},
	}

	add := func(client birpc.ClientConnector, x, y int) {
		var reply int
		if err := client.Call(context.Background(), "TestObj.Add",
			&TestArgs{
				A: x,
				B: y,
			}, &reply); err != nil {
			b.Error(err)
		}
		if reply != 8 {
			b.Errorf("got: %v, want %v", reply, 8)
		}
	}

	computeName := func(codec, strategy string) string {
		var s strings.Builder
		s.WriteString(strategy)
		s.WriteString("_")
		s.WriteString(codec)
		s.WriteString("_")
		s.WriteString(fmt.Sprintf("parallelism=%d", *prl))
		return s.String()
	}

	for _, benchmark := range benchmarks {
		b.Run(computeName(benchmark.codec, benchmark.strategy), func(b *testing.B) {
			pool := setupPool(b, benchmark.codec, benchmark.strategy, 10)
			b.SetParallelism(*prl)
			// b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					add(pool, 3, 5)
				}
			})
		})
	}

}

func setupPool(tb testing.TB, codec, strategy string, noClients int) birpc.ClientConnector {
	var intChan chan context.ClientConnector
	var network, address string
	switch codec {
	case InternalRPC:
		intChan = make(chan context.ClientConnector, 1)
		intChan <- &TestObj{}
	case JSONrpc, GOBrpc:
		ln := serve(tb, "tcp", ":0", codec, new(TestObj))
		network = ln.Addr().Network()
		address = ln.Addr().String()
	default:
		tb.Fatalf("unsupported codec %q", codec)
	}

	if strategy == PoolParallel {
		pool, err := NewRPCParallelClientPool(context.Background(), network, address,
			false, "", "", "", 1, 0, 0, fibDuration, 2*time.Second, 2*time.Second, codec,
			intChan, 100, true, nil)
		if err != nil {
			tb.Fatal(err)
		}
		return pool
	}

	pool := NewRPCPool(strategy, 2*time.Second)
	for range noClients {
		client, err := NewRPCClient(context.Background(), network, address,
			false, "", "", "", 1, 0, 0, fibDuration, 2*time.Second, 2*time.Second, codec,
			intChan, false, nil)
		if err != nil {
			tb.Fatal(err)
		}
		pool.AddClient(client)
	}
	return pool

}
