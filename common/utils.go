package common

func TryClose(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
