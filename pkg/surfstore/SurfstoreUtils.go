package surfstore

import (
	"bufio"
	"io"
	"log"
	"os"
	"reflect"
	"strings"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	var blockStoreAddr string
	err := client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Panicf("Error raised when getting BlockStore addr when sync'ing- %v\n", err)
	}
	log.Printf("Utils - ClientSync - Syncing with server at %v\n", blockStoreAddr)

	// Open base dir
	baseDirStats, err := os.Stat(client.BaseDir)
	if err != nil || !baseDirStats.IsDir() {
		log.Panicf("Error raised when sync'ing - %v\n", err)
	}

	// Open local index (index.db)
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Panicf("Error raised when sync'ing - %v\n", err)
	}

	// Download remote index
	remoteIndex := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remoteIndex)

	// Get list of files
	files, err := os.ReadDir(client.BaseDir)
	if err != nil {
		log.Panicf("Error raised when sync'ing - %v\n", err)
	}

	// Update local index
	for _, file := range files {

		if file.IsDir() || !CheckValidFilename(file.Name()) {
			continue
		}

		// filesHashLists[file.Name()], err = GetFileHashList(ConcatPath(client.BaseDir, file.Name()), client.BlockSize)
		// if err != nil {
		// 	log.Panicf("Error raised when sync'ing - %v\n", err)
		// }

		hList, err := GetFileHashList(ConcatPath(client.BaseDir, file.Name()), client.BlockSize)
		if err != nil {
			log.Panicf("Error raised when sync'ing - %v\n", err)
		}

		// If it is a new file, i.e. not yet in local index
		if _, ok := localIndex[file.Name()]; !ok {
			localIndex[file.Name()] = &FileMetaData{
				Filename:      file.Name(),
				Version:       int32(1),
				BlockHashList: hList,
			}
		} else if !IsEqualHashLists(hList, localIndex[file.Name()].BlockHashList) {
			// Updated content, i.e. updated hashList
			localIndex[file.Name()].BlockHashList = hList
			localIndex[file.Name()].Version++
		}

	}

	// Check deleted files
	for fileName, localMetaData := range localIndex {
		if FileDoesNotExist(ConcatPath(client.BaseDir, fileName)) {
			if !IsDeletedFile(localMetaData) {
				// If originally not "deleted file", update meta
				localMetaData.Version++
				localMetaData.BlockHashList = []string{"0"}
			}
		}
	}
	// Compare local index with remote index

	// Case 1: Present in remote index, not present in local index/base dir
	// => Download file, update local index

	// Case 2: New files in local base dir, but not in local index/remote index
	// => Upload file to server, update remote index, if success then update local index

	// log.Println(localIndex)
	// log.Println(remoteIndex)

	for fileName, remoteMetaData := range remoteIndex {
		if localMetaData, ok := localIndex[fileName]; !ok {
			// File not exist at local base dir
			localIndex[fileName] = &FileMetaData{}
			DownloadFile(client, blockStoreAddr, localIndex[fileName], remoteMetaData)
		} else {
			// File exist at local base dir,
			// however only need download only if remote ver >= local ver
			// Example: local modified (ver 3 -> 4), but remote ver 4 (someone sync-ed update)
			// By the rules, we download remote content and overwrite local data
			if remoteMetaData.Version >= localMetaData.Version {
				DownloadFile(client, blockStoreAddr, localMetaData, remoteMetaData)
			}
		}
	}

	for fileName, localMetaData := range localIndex {
		if remoteMetaData, ok := remoteIndex[fileName]; !ok {
			// File not exist at remote
			e := UploadFile(client, blockStoreAddr, localMetaData, &FileMetaData{Version: 0})
			if e != nil {
				log.Panicf("Error raised when sync'ing - %v\n", err)
			}
		} else {
			// File exist at remote,
			// however only update if local has newer ver # than remote ver #
			if localMetaData.Version >= remoteMetaData.Version {
				e := UploadFile(client, blockStoreAddr, localMetaData, remoteMetaData)
				if e != nil {
					log.Panicf("Error raised when sync'ing - %v\n", err)
				}
			}
		}
	}

	WriteMetaFile(localIndex, client.BaseDir)

}

// Check if two hash lists are completely equal
func IsEqualHashLists(l1 []string, l2 []string) bool {
	return reflect.DeepEqual(l1, l2)
}

// Check if two meta data has same version and hashlists
func IsEqualMeta(m1 *FileMetaData, m2 *FileMetaData) bool {
	return m1.Version == m2.Version &&
		IsEqualHashLists(m1.BlockHashList, m2.BlockHashList)
}

// Compute the file's hash list
func GetFileHashList(path string, blockSize int) ([]string, error) {

	fileStat, err := os.Stat(path)
	if err != nil {
		return []string{}, err
	}
	if fileStat.Size() == 0 {
		return []string{"-1"}, nil
	}

	// Open file
	f, err := os.Open(path)
	if err != nil {
		// log.Panicf("Error raised when sync'ing - %v\n", err)
		return []string{}, err
	}

	// Compute file's hash list
	hashListLocal := make([]string, 0)

	for {
		buf := make([]byte, blockSize)
		n, err := io.ReadFull(f, buf)
		log.Printf("Utils - GetFileHashList - Read %v bytes from %v", n, path)
		if err != nil && err != io.ErrUnexpectedEOF {
			if err == io.EOF {
				break
			}
			// log.Panicf("Error raised when sync'ing - %v\n", err)
			return []string{}, err
		}

		hashListLocal = append(hashListLocal, GetBlockHashString(buf[:n]))
	}

	return hashListLocal, nil
}

// Check if filename is valid
func CheckValidFilename(fileName string) bool {
	return !(fileName == "index.db" || strings.Contains(fileName, ",") || strings.Contains(fileName, "/"))
}

// Download file from remote
func DownloadFile(client RPCClient, blockStoreAddr string, localMetaData *FileMetaData, remoteMetaData *FileMetaData) error {

	path := ConcatPath(client.BaseDir, remoteMetaData.Filename)

	log.Printf("Utils - DownloadFile - Start downloading file %v\n", path)

	if IsEqualMeta(localMetaData, remoteMetaData) {
		log.Printf("Utils - DownloadFile - Local and remote has the same copy, terminate downlaod.\n")
		return nil
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = os.Stat(path)
	if err != nil {
		return err
	}

	// If file is deleted on remote
	if len(remoteMetaData.BlockHashList) == 1 {

		if remoteMetaData.BlockHashList[0] == "0" {
			if err := os.Remove(path); err != nil {
				return err
			}
			// *localMetaData = *remoteMetaData
			*localMetaData = FileMetaData{
				Filename:      remoteMetaData.Filename,
				Version:       remoteMetaData.Version,
				BlockHashList: []string{"0"},
			}
			return nil
		}

		if remoteMetaData.BlockHashList[0] == "-1" {
			return nil
		}
	}
	// Create file at local base dir
	newLocalFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer newLocalFile.Close()

	// Download and write to file
	writer := bufio.NewWriter(newLocalFile)
	var tmpBlock Block
	for _, h := range remoteMetaData.BlockHashList {
		err = client.GetBlock(h, blockStoreAddr, &tmpBlock)
		if err != nil {
			log.Panicf("Error raised when sync'ing - %v\n", err)
		}
		writer.Write(tmpBlock.BlockData[:tmpBlock.BlockSize])
	}
	writer.Flush()

	*localMetaData = FileMetaData{
		Filename:      remoteMetaData.Filename,
		Version:       remoteMetaData.Version,
		BlockHashList: remoteMetaData.BlockHashList,
	}

	return nil
}

// Check deleted file
func FileDoesNotExist(path string) bool {
	_, ok := os.Stat(path)
	return os.IsNotExist(ok)
}

// Check meta to see if file is deleted
func IsDeletedFile(meta *FileMetaData) bool {
	return len(meta.BlockHashList) == 1 && meta.BlockHashList[0] == "0"
}

// Upload file to remote
func UploadFile(client RPCClient, blockStoreAddr string, localMetaData *FileMetaData, remoteMetaData *FileMetaData) error {

	path := ConcatPath(client.BaseDir, localMetaData.Filename)

	log.Printf("Utils - UploadFile - Start uploading file %v\n", path)

	if remoteMetaData.Version != 0 && IsEqualMeta(localMetaData, remoteMetaData) {
		log.Printf("Utils - UploadFile - Local and remote has the same copy, terminate downlaod.\n")
		return nil
	}

	// Special case - deleted file
	var latestVersion int32
	if IsDeletedFile(localMetaData) {
		err := client.UpdateFile(localMetaData, &latestVersion)
		if err != nil {
			return err
		} else {
			localMetaData.Version = latestVersion
			return nil
		}
	}

	// Normal case
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Generate byte blocks and hashes
	// log.Println("here uploading")
	for {
		buf := make([]byte, client.BlockSize)
		n, err := io.ReadFull(file, buf)
		log.Printf("Utils - UploadFile - Read %v bytes from %v", n, path)
		// ErrUnexpectedEOF => read len([]byte) < blockSize
		if err != nil && err != io.ErrUnexpectedEOF {
			if err == io.EOF {
				// Finish reading and sending file
				break
			}
			// Didn't finish reading file
			return err
		}

		block := Block{BlockData: buf, BlockSize: int32(n)}

		var succ bool
		if err := client.PutBlock(&block, blockStoreAddr, &succ); err != nil {
			return err
		}
	}

	// Update remote meta
	if err := client.UpdateFile(localMetaData, &latestVersion); err != nil {
		return err
	}

	localMetaData.Version = latestVersion
	return nil

}

// Debug use: check file size
// Use carefully - doesn't do error check
func GetFileSize(path string) {
	f, _ := os.Open(path)
	stat, _ := os.Stat(f.Name())
	log.Printf("DEBUG - GetFileSize - File %v has file size %v\n", path, stat.Size())
}
