package rpcclient

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

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
	rpcppool, err := NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 2, false, nil)
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
	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 2, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	rpcppool.codec = "Not a supported codec"
	if err = rpcppool.Call(context.Background(), "", "", &reply); err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}

	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, "Not a supported codec", internalChan, 2, false, nil)
	if err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}

	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, InternalRPC, nil, 2, false, nil)
	if err != ErrInternallyDisconnected {
		t.Errorf("Expected error: %s received: %v", ErrInternallyDisconnected, err)
	}

	close(internalChan)
	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "sleep"}
	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Millisecond, 70*time.Millisecond, InternalRPC, internalChan, 2, false, nil)
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
	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 3, true, nil)
	if err != nil {
		t.Fatal(err)
	} else if rpcppool.counter != 3 {
		t.Errorf("Expected: the counter to be 3 received: %v", rpcppool.counter)
	} else if len(rpcppool.connectionsChan) != 3 {
		t.Errorf("Expected: 1 received: %v", len(rpcppool.connectionsChan))
	}

	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 3, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	rpcppool.codec = "Not a supported codec"
	if err = rpcppool.initConns(); err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}
}

func TestIsNetworkError(t *testing.T) {
	var err error
	if IsNetworkError(err) {
		t.Errorf("Nill error should not be consider a network error")
	}
	err = &net.OpError{Err: syscall.ECONNRESET}
	if !IsNetworkError(err) {
		t.Errorf("syscall.ECONNRESET should be consider a network error")
	}
	err = fmt.Errorf("NOT_FOUND")
	if IsNetworkError(err) {
		t.Errorf("%s error should not be consider a network error", err)
	}
	err = ErrDisconnected
	if !IsNetworkError(err) {
		t.Errorf("%s error should be consider a network error", err)
	}
	err = new(net.DNSError)
	if !IsNetworkError(err) {
		t.Errorf("%s error should be consider a network error", err)
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
	client, err := NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, 2, false, nil)
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
	fib := Fib()
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
	if _, err := NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, "Not a supported codec", nil, true, nil); err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}
	if _, err := NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, InternalRPC, nil, true, nil); err != ErrInternallyDisconnected {
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
		internalChan: nil,
	}
	client, err := NewRPCClient("transport", "addr", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, JSONrpc, nil, true, nil)
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
	_, err = NewRPCClient("transport", "addr", false, "", "", "", 5, 10,
		5*time.Millisecond, 5*time.Millisecond, JSONrpc, nil, false, nil)
	if err == nil {
		t.Errorf("Expected connection error received:%v", err)
	}

	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- nil
	if _, err := NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, false, nil); err != ErrDisconnected {
		t.Errorf("Expected error: %s received: %v", ErrDisconnected, err)
	}
	internalChan = make(chan birpc.ClientConnector, 1)
	if _, err := NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, time.Millisecond, InternalRPC, internalChan, false, nil); err != context.DeadlineExceeded {
		t.Errorf("Expected error: %s received: %v", context.DeadlineExceeded, err)
	}
	if client, err := NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, time.Millisecond, HTTPjson, internalChan, false, nil); err != nil {
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
	client, err = NewRPCClient("tcp", addr, false, "", "", "", 5, 10,
		5*time.Millisecond, 5*time.Millisecond, JSONrpc, nil, false, nil)
	if err != nil {
		t.Error(err)
	} else if !client.isConnected() {
		t.Errorf("Expected to start the connection")
	}
	client.disconnect()
	if client.isConnected() {
		t.Errorf("Expected to stop the connection after disconnect")
	}

	client, err = NewRPCClient("tcp", addr, false, "", "", "", 5, 10,
		5*time.Millisecond, 5*time.Millisecond, BiRPCJSON, nil, false, new(MockRPCClient))
	if err != nil {
		t.Error(err)
	} else if !client.isConnected() {
		t.Errorf("Expected to start the connection")
	}
	client.disconnect()
	if client.isConnected() {
		t.Errorf("Expected to stop the connection after disconnect")
	}

	client, err = NewRPCClient("tcp", addr, false, "", "", "", 5, 10,
		time.Millisecond, time.Millisecond, GOBrpc, nil, false, nil)
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
	client, err = NewRPCClient("tcp", addr, false, "", "", "", 5, 10,
		5*time.Millisecond, 5*time.Millisecond, BiRPCGOB, nil, false, new(MockRPCClient))
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
	client, err := NewRPCClient("transport", "addr", false, "", "", "", 5, 10,
		10*time.Millisecond, 5*time.Millisecond, InternalRPC, internalChan, true, nil)
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
	client, err = NewRPCClient("transport", "addr", false, "", "", "", 1, 1,
		10*time.Millisecond, 5*time.Millisecond, InternalRPC, internalChan, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	client.connection = nil
	if err = client.Call(context.Background(), "", "", reply); err != ErrDisconnected {
		t.Errorf("Expected error: %s received: %v", ErrDisconnected, err)
	}
	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- &MockRPCClient{id: "sleep"}
	client, err = NewRPCClient("transport", "addr", false, "", "", "", 1, 1,
		10*time.Millisecond, 5*time.Millisecond, InternalRPC, internalChan, false, nil)
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
	cl, err := NewRPCClient("", "", false, "", "", "", 1, 1,
		time.Millisecond, 25*time.Millisecond, InternalRPC, internalChan, true, nil)
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
	rpcc, err := NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, InternalRPC, internalChan, false, nil)
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
	p, err := NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, BiRPCInternal, internalChan, false, birpcClient)
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
	_, err = NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 0, BiRPCInternal, internalChan, false, &MockRPCClient{id: "2"})
	if err != context.DeadlineExceeded {
		t.Errorf("Expected error %s received:%v ", context.DeadlineExceeded, err)
	}

	internalChan <- nil
	_, err = NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 50*time.Millisecond, BiRPCInternal, internalChan, false, &MockRPCClient{id: "2"})
	if err != ErrDisconnected {
		t.Errorf("Expected error %s received:%v ", ErrDisconnected, err)
	}

	close(internalChan)

	internalChan = make(chan birpc.ClientConnector, 1)
	internalChan <- &RPCParallelClientPool{}

	_, err = NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 0, BiRPCJSON, internalChan, false, nil)
	if err != ErrUnsupportedBiRPC {
		t.Errorf("Expected error %s received:%v ", ErrUnsupportedBiRPC, err)
	}

	_, err = NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 0, BiRPCInternal, internalChan, false, nil)
	if err != ErrUnsupportedBiRPC {
		t.Errorf("Expected error %s received:%v ", ErrUnsupportedBiRPC, err)
	}

	_, err = NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Millisecond, 0, BiRPCInternal, nil, false, nil)
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
