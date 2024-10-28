# CGRateS RPCClient

A Go RPC client library supporting multiple codecs and pooling strategies. Part of the [CGRateS](http://www.cgrates.org/) project. The library provides thread-safe operations, automatic failover with reconnection support, and optional TLS encryption.

## Installation

```bash
go get github.com/cgrates/rpcclient
```

## Features

- Multiple codec support:
  - Standard RPC: JSON (`*json`), GOB (`*gob`), HTTP JSON (`*http_jsonrpc`)
  - Bi-directional RPC: JSON (`*birpc_json`), GOB (`*birpc_gob`)
  - Internal direct calls (`*internal`, `*birpc_internal`) for objects implementing `birpc.ClientConnector`
- Connection pooling with various dispatch strategies
- Automatic failover for network errors, timeouts, and missing service errors

## Pool Strategies

- `*first`: Uses first available connection, fails over on network/timeout/missing service errors
- `*next`: Round-robin between connections with same failover as `*first`
- `*random`: Random connection selection with same failover as `*first`
- `*first_positive`: Tries connections in order until getting any successful response (no error)
- `*first_positive_async`: Async version of `*first_positive`
- `*broadcast`: Sends to all connections, returns first successful response
- `*broadcast_sync`: Sends to all, waits for completion, logs errors that wouldn't trigger failover in `*first`
- `*broadcast_async`: Sends to all without waiting for responses or error handling
- `*parallel`: Connection pool that creates and reuses connections up to a maximum limit, either pre-initialized or on demand

## Support

Join [CGRateS](http://www.cgrates.org/) on Google Groups [here](https://groups.google.com/forum/#!forum/cgrates).

## License

RpcClient is released under the [MIT License](http://www.opensource.org/licenses/mit-license.php).

Copyright (C) ITsysCOM GmbH. All Rights Reserved.
