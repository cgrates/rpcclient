package rpcclient

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"testing"
	"time"
)

/*
Integration tests. Needs system permissions since it will start servers.
Run it with go test -integration
*/

// Settings here
var (
	SrvAddr         = "localhost"
	SrvPort         = 12012
	OK              = "OK"
	PING            = "PING"
	PONG            = "PONG"
	testIntegration = flag.Bool("integration", false, "Perform the tests in integration mode, not by default.") // This flag will be passed here via "go test -local" args
	waitExecute     = flag.Int("wait_rater", 100, "Number of miliseconds to wait for rater to start and cache")
)

type RPC struct {
	srvID string
	val   string
}

func (s *RPC) Cache(val string, reply *string) error {
	s.val = val
	*reply = OK
	return nil
}

func (s *RPC) GetCached(ign string, reply *string) error {
	*reply = s.val
	return nil
}

func (s *RPC) GetServerID(ign string, reply *string) error {
	*reply = s.srvID
	return nil
}

func startServer(srvID, listenAddr string, shutdownTimer time.Duration) error {
	api := new(RPC)
	api.srvID = srvID
	server := rpc.NewServer()
	server.Register(api)
	oldMux := http.DefaultServeMux // Work around so we can register multiple endpoints on the same path
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux
	l, e := net.Listen("tcp", listenAddr)
	if e != nil {
		return e
	}
	if shutdownTimer != 0 {
		go func() { // Scheduled connection shutdown
			time.Sleep(shutdownTimer)
			log.Printf("Server %s, stop listening on: %s\n", srvID, listenAddr)
			l.Close()
		}()
	}
	log.Printf("Server %s, start listening on: %s\n", srvID, listenAddr)
	for {
		if conn, err := l.Accept(); err != nil {
			log.Printf("Server %s, error: %s listening on: %s\n", srvID, err.Error(), listenAddr)
			return err
		} else {
			server.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}
	panic("Should never get here")
}

func TestITPoolFirst(t *testing.T) {
	if !*testIntegration {
		return
	}
	go startServer("srv1", fmt.Sprintf("%s:%d", SrvAddr, SrvPort), 0)
	go startServer("srv2", fmt.Sprintf("%s:%d", SrvAddr, SrvPort+1), 0)
	clnt1, err := NewRpcClient("tcp", fmt.Sprintf("%s:%d", SrvAddr, SrvPort), 1, 0, JSON_RPC, nil)
	if err != nil {
		t.Fatal(err)
	}
	clnt2, err := NewRpcClient("tcp", fmt.Sprintf("%s:%d", SrvAddr, SrvPort+1), 1, 0, JSON_RPC, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := &RpcClientPool{
		transmissionType: POOL_FIRST,
		connections:      []RpcClientConnection{clnt1, clnt2},
	}
	eSrvID := "srv1"
	var srvID string
	if err := p.Call("RPC.GetServerID", "", &srvID); err != nil {
		t.Error(err)
	} else if srvID != eSrvID {
		t.Errorf("Expecting: %s, received: %s", eSrvID, srvID)
	}
	if err := p.Call("RPC.GetServerID", "", &srvID); err != nil { // Second time should return the same
		t.Error(err)
	} else if srvID != eSrvID {
		t.Errorf("Expecting: %s, received: %s", eSrvID, srvID)
	}
}
