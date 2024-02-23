package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	panic("todo")
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	panic("todo")
}
