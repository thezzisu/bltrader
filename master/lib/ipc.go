package lib

type IPCRequest struct {
	Cb chan<- interface{}

	// 0 - exit
	// 1 - query
	Method  int
	Payload interface{}
}

const (
	IPC_EXIT        = 0
	IPC_LOG         = 1
	IPC_STOCK_QUERY = 12
	IPC_STOCK_STORE = 13
)
