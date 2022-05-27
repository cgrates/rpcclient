package rpcclient

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/cgrates/birpc"
	"github.com/cgrates/birpc/context"
	"github.com/cgrates/birpc/jsonrpc"
)

var (
	noClinets int64 = 1000
)

func init() {
	birpc.Register(&MockRPCClient{})
}

func startRPCSertver(addr string) (err error) {
	var l net.Listener
	if l, err = net.Listen("tcp", addr); err != nil {
		return
	}
	for {
		var conn io.ReadWriteCloser
		if conn, err = l.Accept(); err != nil {
			return
		}
		go jsonrpc.ServeConn(conn)
	}
}
func BenchmarkNewRPCParallelClientPoolWithoutInit(b *testing.B) {
	addr := "localhost:2012"
	go startRPCSertver(addr)
	client, err := NewRPCParallelClientPool(context.Background(), "tcp", addr, false,
		"", "", "", 5, 5, 0, fibDuration, 5*time.Millisecond, 60*time.Millisecond, JSONrpc,
		nil, noClinets, false, nil)
	if err != nil {
		b.Error(err)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var reply string
		for pb.Next() {
			if err = client.Call(context.Background(), "MockRPCClient.Echo", "", &reply); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkNewRPCParallelClientPoolWithInit(b *testing.B) {
	addr := "localhost:2013"
	go startRPCSertver(addr)
	client, err := NewRPCParallelClientPool(context.Background(), "tcp", addr, false,
		"", "", "", 5, 5, 0, fibDuration, 5*time.Millisecond, 60*time.Millisecond, JSONrpc,
		nil, noClinets, true, nil)
	if err != nil {
		b.Error(err)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var reply string
		for pb.Next() {
			if err = client.Call(context.Background(), "MockRPCClient.Echo", "", &reply); err != nil {
				b.Error(err)
			}
		}
	})
}
