package rpcclient

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"
)

type mockRPCClient struct {
	id string
}

func (m *mockRPCClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	switch m.id {
	case "offline":
		return ErrReqUnsynchronized
	case "error":
		return errors.New("Not Found")
	case "sleep":
		time.Sleep(50 * time.Millisecond)
		fallthrough
	default:
		*reply.(*string) += m.id
		return nil
	}
}

func TestPoolFirst(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolFirst,
		connections: []ClientConnector{
			&mockRPCClient{id: "1"},
			&mockRPCClient{id: "2"},
			&mockRPCClient{id: "3"},
			&mockRPCClient{id: "4"},
		},
	}
	var response string
	p.Call("", "", &response)
	if response != "1" {
		t.Error("Error calling client: ", response)
	}
	p = &RPCPool{
		transmissionType: PoolFirst,
		connections: []ClientConnector{
			&mockRPCClient{id: "offline"},
			&mockRPCClient{id: "2"},
			&mockRPCClient{id: "3"},
			&mockRPCClient{id: "4"},
		},
	}
	p.Call("", "", &response)
	if response != "12" {
		t.Error("Error calling client: ", response)
	}
}

func TestPoolNext(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolNext,
		connections: []ClientConnector{
			&mockRPCClient{id: "1"},
			&mockRPCClient{id: "2"},
			&mockRPCClient{id: "3"},
			&mockRPCClient{id: "4"},
		},
	}
	var response string
	p.Call("", "", &response)
	if response != "1" {
		t.Error("Error calling client: ", response)
	}

	p.Call("", "", &response)
	if response != "12" {
		t.Error("Error calling client: ", response)
	}
}

func TestPoolBrodcast(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolBroadcast,
		connections: []ClientConnector{
			&mockRPCClient{id: "1"},
			&mockRPCClient{id: "2"},
			&mockRPCClient{id: "3"},
			&mockRPCClient{id: "4"},
		},
	}
	var response string
	if err := p.Call("", "", &response); err != nil {
		t.Error("Got error: ", err)
	}
	if len(response) != 1 {
		t.Error("Error calling client: ", response)
	}
}

func TestPoolRANDOM(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolRandom,
		connections: []ClientConnector{
			&mockRPCClient{id: "1"},
			&mockRPCClient{id: "2"},
			&mockRPCClient{id: "3"},
			&mockRPCClient{id: "4"},
		},
	}
	m := make(map[string]struct{}, 4)
	for i := 0; i < 100; i++ {
		var response string
		if err := p.Call("", "", &response); err != nil {
			t.Error("Got error: ", err)
		}
		m[response] = struct{}{}
	}
	if len(m) < 4 { // should use them all
		t.Error("Error calling client: ", m)
	}
}

func TestPoolFirstPositive(t *testing.T) {
	p := &RPCPool{
		transmissionType: PoolFirstPositive,
		connections: []ClientConnector{
			&mockRPCClient{id: "error"},
			&mockRPCClient{id: "error"},
			&mockRPCClient{id: "3"},
			&mockRPCClient{id: "4"},
		},
	}
	var response string
	p.Call("", "", &response)
	if response != "3" {
		t.Error("Error calling client: ", response)
	}

	p = &RPCPool{
		transmissionType: PoolFirstPositive,
		connections: []ClientConnector{
			&mockRPCClient{id: "error"},
			&mockRPCClient{id: "2"},
			&mockRPCClient{id: "error"},
			&mockRPCClient{id: "4"},
		},
	}
	response = ""
	p.Call("", "", &response)
	if response != "2" {
		t.Error("Error calling client: ", response)
	}
}

func TestNewRpcParallelClientPool(t *testing.T) {
	internalChan := make(chan ClientConnector, 1)
	internalChan <- &mockRPCClient{id: "1"}
	rpcppool, err := NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Second, 50*time.Millisecond, InternalRPC, internalChan, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	var reply string
	if err = rpcppool.Call("", "", &reply); err != nil {
		t.Error(err)
	} else if reply != "1" {
		t.Errorf("Expected: \"1\" received: %q", reply)
	} else if rpcppool.counter != 1 {
		t.Errorf("Expected: the counter to be 1 received: %v", rpcppool.counter)
	} else if len(rpcppool.connectionsChan) != 1 {
		t.Errorf("Expected: 1 received: %v", len(rpcppool.connectionsChan))
	}
	reply = ""
	if err = rpcppool.Call("", "", &reply); err != nil { // this should take the connection from earlier
		t.Error(err)
	} else if reply != "1" {
		t.Errorf("Expected: \"1\" received: %q", reply)
	} else if rpcppool.counter != 1 {
		t.Errorf("Expected: the counter to be 1 received: %v", rpcppool.counter)
	} else if len(rpcppool.connectionsChan) != 1 {
		t.Errorf("Expected: 1 received: %v", len(rpcppool.connectionsChan))
	}
	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Second, 50*time.Millisecond, InternalRPC, internalChan, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	rpcppool.codec = "Not a supported codec"
	if err = rpcppool.Call("", "", &reply); err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}

	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Second, 50*time.Millisecond, "Not a supported codec", internalChan, 2, false)
	if err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}

	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Second, 50*time.Millisecond, InternalRPC, nil, 2, false)
	if err != ErrInternallyDisconnected {
		t.Errorf("Expected error: %s received: %v", ErrInternallyDisconnected, err)
	}

	close(internalChan)
	internalChan = make(chan ClientConnector, 1)
	internalChan <- &mockRPCClient{id: "sleep"}
	rpcppool, err = NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Second, 70*time.Millisecond, InternalRPC, internalChan, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	parlallel := func(t *testing.T) {
		reply := ""
		if err = rpcppool.Call("", "", &reply); err != nil { // this should take the connection from earlier
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
}

func TestIsNetworkError(t *testing.T) {
	var err error
	if isNetworkError(err) {
		t.Errorf("Nill error should not be consider a network error")
	}
	err = &net.OpError{Err: syscall.ECONNRESET}
	if !isNetworkError(err) {
		t.Errorf("syscall.ECONNRESET should be consider a network error")
	}
	err = fmt.Errorf("NOT_FOUND")
	if isNetworkError(err) {
		t.Errorf("%s error should not be consider a network error", err)
	}
	err = ErrDisconnected
	if !isNetworkError(err) {
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
		replyTimeout:     time.Second,
	}

	pool := NewRPCPool(PoolBroadcast, time.Second)
	if !reflect.DeepEqual(exp, pool) {
		t.Errorf("Expected: %v received: %v", exp, pool)
	}
	internalChan := make(chan ClientConnector, 1)
	internalChan <- &mockRPCClient{id: "1"}
	client, err := NewRPCParallelClientPool("", "", false, "", "", "", 5, 10,
		time.Second, 50*time.Millisecond, InternalRPC, internalChan, 2, false)
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
		time.Second, 50*time.Millisecond, "Not a supported codec", nil, true); err != ErrUnsupportedCodec {
		t.Errorf("Expected error: %s received: %v", ErrUnsupportedCodec, err)
	}
	if _, err := NewRPCClient("", "", false, "", "", "", 5, 10,
		time.Second, 50*time.Millisecond, InternalRPC, nil, true); err != ErrInternallyDisconnected {
		t.Errorf("Expected error: %s received: %v", ErrInternallyDisconnected, err)
	}

	internalChan := make(chan ClientConnector, 1)
	internalChan <- &mockRPCClient{id: "1"}
	exp := &RPCClient{
		transport:    "transport",
		tls:          false,
		address:      "addr",
		keyPath:      "",
		certPath:     "",
		caPath:       "",
		reconnects:   10,
		connTimeout:  time.Second,
		replyTimeout: 50 * time.Millisecond,
		codec:        JSONrpc,
		internalChan: nil,
	}
	client, err := NewRPCClient("transport", "addr", false, "", "", "", 5, 10,
		time.Second, 50*time.Millisecond, JSONrpc, nil, true)
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
}
