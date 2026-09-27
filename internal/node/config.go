package node

type Config struct {
	ID      string
	Address string
	Peers   map[string]string
}

func NewConfig(id string, address string, peers map[string]string) Config {
	return Config{
		ID:      id,
		Address: address,
		Peers:   peers,
	}
}
