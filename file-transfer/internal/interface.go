package internal

type Server interface {
	Start() error
	Close()
}

type Client interface {
	Transfer(string) error
	Close()
}
