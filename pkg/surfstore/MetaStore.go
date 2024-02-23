package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData // filename -> (filename, version, hashList)
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	oldMeta, ok := m.FileMetaMap[fileMetaData.Filename]
	if !ok {
		m.FileMetaMap[fileMetaData.Filename] = &FileMetaData{
			Filename:      fileMetaData.Filename,
			Version:       fileMetaData.Version,
			BlockHashList: fileMetaData.BlockHashList,
		}
		log.Println("MetaStore - Update file returned succecss case 1")
		return &Version{Version: fileMetaData.Version}, nil
	}

	if fileMetaData.Version < oldMeta.Version ||
		(fileMetaData.Version == oldMeta.Version &&
			!IsEqualHashLists(fileMetaData.BlockHashList, oldMeta.BlockHashList)) {
		// Either modified, or not modified
		log.Println("MetaStore - Update file returned error case 2")
		return &Version{Version: -1}, fmt.Errorf("version is too old, received version = %v, current remote version %v", fileMetaData.Version, oldMeta.Version)
	}

	// Update with new list
	m.FileMetaMap[fileMetaData.Filename].BlockHashList = fileMetaData.BlockHashList
	(m.FileMetaMap[fileMetaData.Filename].Version)++

	log.Println("MetaStore - Update file returned success case 3")
	return &Version{Version: m.FileMetaMap[fileMetaData.Filename].Version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	panic("todo")
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	panic("todo")
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
