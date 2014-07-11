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
	"flag"
	"os/exec"
	"path"
	"reflect"
	"testing"
	"time"
)

var testLocal = flag.Bool("local", false, "Perform the tests only on local test environment, not by default.")
var dataDir = flag.String("data_dir", "/usr/share/cgrates", "CGR data dir path here")
var startDelay = flag.Int("delay_start", 300, "Number of miliseconds to it for rater to start and cache")
var cfgPath = path.Join(*dataDir, "conf", "samples", "mediator_test1.cfg")

var rpcClient *RpcClient
var err error


type AttrCacheStats struct { // Add in the future filters here maybe so we avoid counting complete cache
}

type CacheStats struct {
	Destinations    int64
	RatingPlans     int64
	RatingProfiles  int64
	Actions         int64
	SharedGroups    int64
	RatingAliases   int64
	AccountAliases  int64
	DerivedChargers int64
}

var eCacheStats CacheStats = CacheStats{Destinations:4, RatingPlans:3, RatingProfiles:3,
	Actions:8, SharedGroups:1, RatingAliases:0, AccountAliases:0, DerivedChargers:3}

func startEngine() error {
	enginePath, err := exec.LookPath("cgr-engine")
	if err != nil {
		return err
	}
	exec.Command("pkill", "cgr-engine").Run() // Just to make sure another one is not running, bit brutal maybe we can fine tune it
	engine := exec.Command(enginePath, "-config", cfgPath)
	if err := engine.Start(); err != nil {
		return err
	}
	time.Sleep(time.Duration(*startDelay) * time.Millisecond) // Give time to rater to fire up
	return nil
}

// Make sure client connects and is able to query cached data, using various methods
func TestNewRpcClient(t *testing.T) {
	if !*testLocal {
		return
	}
	if err := startEngine(); err != nil {
		t.Fatal("Cannot start cgr-engine: ", err.Error())
	}
	var cacheStats CacheStats
	if rpcClient, err = NewRpcClient("tcp", "localhost", 2013, 0, GOB_RPC); err != nil {
		t.Error("Unexpected error: ", err)
	} else if rpcClient.connection == nil {
		t.Error("Not connected")
	}
	if err := rpcClient.Call("ApierV1.GetCacheStats", AttrCacheStats{}, &cacheStats); err != nil {
		t.Error(err.Error())
	} else if !reflect.DeepEqual(eCacheStats, cacheStats) {
		t.Errorf("Expecting: %+v, received: %+v", eCacheStats, cacheStats)
	}
	if rpcClient, err = NewRpcClient("tcp", "localhost", 2012, 0, JSON_RPC); err != nil {
		t.Error("Unexpected error: ", err)
	} else if rpcClient.connection == nil {
		t.Error("Not connected")
	}
	if err := rpcClient.Call("ApierV1.GetCacheStats", AttrCacheStats{}, &cacheStats); err != nil {
		t.Error(err.Error())
	} else if !reflect.DeepEqual(eCacheStats, cacheStats) {
		t.Errorf("Expecting: %+v, received: %+v", eCacheStats, cacheStats)
	}
}

func TestCallWithConnectionLoss(t *testing.T) {
	if !*testLocal {
		return
	}
	if err := startEngine(); err != nil {
		t.Fatal("Cannot start cgr-engine: ", err.Error())
	}
	var cacheStats CacheStats
	if err := rpcClient.Call("ApierV1.GetCacheStats", AttrCacheStats{}, &cacheStats); err == nil {
		t.Error("Previous connection should be lost due to restart")
	}
}

func TestReconnect(t *testing.T) {
        if !*testLocal {
                return
        }
        var cacheStats CacheStats
        if rpcClient, err = NewRpcClient("tcp", "localhost", 2013, 1, GOB_RPC); err != nil {
                t.Error("Unexpected error: ", err)
        } else if rpcClient.connection == nil {
                t.Error("Not connected")
        }
        if err := rpcClient.Call("ApierV1.GetCacheStats", AttrCacheStats{}, &cacheStats); err != nil {
                t.Error(err.Error())
        } else if !reflect.DeepEqual(eCacheStats, cacheStats) {
                t.Errorf("Expecting: %+v, received: %+v", eCacheStats, cacheStats)
        }
}
