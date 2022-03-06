package lib

type IPCRequest struct {
	Cb chan<- interface{}

	// 0 - exit
	// 1 - query
	Method  int
	Payload interface{}
}

const (
	IPC_EXIT = 0
	IPC_LOG  = 1
)