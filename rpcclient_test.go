package rpcclient

import (
	"errors"
	"testing"
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
	default:
		*reply.(*string) += m.id
		return nil
	}
}

func TestPoolFirst(t *testing.T) {
	p := &RpcClientPool{
		transmissionType: POOL_FIRST,
		connections: []RpcClientConnection{
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
	p = &RpcClientPool{
		transmissionType: POOL_FIRST,
		connections: []RpcClientConnection{
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
	p := &RpcClientPool{
		transmissionType: POOL_NEXT,
		connections: []RpcClientConnection{
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
	p := &RpcClientPool{
		transmissionType: POOL_BROADCAST,
		connections: []RpcClientConnection{
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
	/*time.Sleep(1 * time.Millisecond)
	if len(response) != 1 {
		t.Error("Error calling client: ", response)
	}
	*/
}

func TestPoolRANDOM(t *testing.T) {
	p := &RpcClientPool{
		transmissionType: POOL_RANDOM,
		connections: []RpcClientConnection{
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
	p := &RpcClientPool{
		transmissionType: POOL_FIRST_POSITIVE,
		connections: []RpcClientConnection{
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

	p = &RpcClientPool{
		transmissionType: POOL_FIRST_POSITIVE,
		connections: []RpcClientConnection{
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
