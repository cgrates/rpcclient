package rpcclient

import (
	"testing"
	//"time"
)

type MockRpcClient struct {
	id string
}

func (m *MockRpcClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	if m.id == "offline" {
		return ErrReqUnsynchronized
	}
	*reply.(*string) += m.id
	return nil
}

func TestPoolFirst(t *testing.T) {
	p := &RpcClientPool{
		transmissionType: POOL_FIRST,
		connections: []RpcClientConnection{
			&MockRpcClient{id: "1"},
			&MockRpcClient{id: "2"},
			&MockRpcClient{id: "3"},
			&MockRpcClient{id: "4"},
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
			&MockRpcClient{id: "offline"},
			&MockRpcClient{id: "2"},
			&MockRpcClient{id: "3"},
			&MockRpcClient{id: "4"},
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
			&MockRpcClient{id: "1"},
			&MockRpcClient{id: "2"},
			&MockRpcClient{id: "3"},
			&MockRpcClient{id: "4"},
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
			&MockRpcClient{id: "1"},
			&MockRpcClient{id: "2"},
			&MockRpcClient{id: "3"},
			&MockRpcClient{id: "4"},
		},
	}
	var response string
	if err := p.Call("", "", &response); err.Error() != ErrReplyTimeout.Error() {
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
			&MockRpcClient{id: "1"},
			&MockRpcClient{id: "2"},
			&MockRpcClient{id: "3"},
			&MockRpcClient{id: "4"},
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
