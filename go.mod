module github.com/cgrates/rpcclient

go 1.16

replace github.com/cgrates/rpc => ../rpc

replace github.com/cgrates/birpc => ../birpc

require (
	github.com/cgrates/birpc v0.0.0-20210220005819-4a29bc83afe1
	github.com/cgrates/rpc v1.3.1-0.20210402104943-a0214560d22c

)
