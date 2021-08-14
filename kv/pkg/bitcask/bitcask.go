package bitcask

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	_dataFileSuffix string = ".db"
	_activeFile     string = ".active"
	_mergeFile      string = ".merge"
)

var (
	FILE_ERROR        = errors.New("bitcask: file error")
	GET_FILE_ID_ERROR = errors.New("bitcask: get file id error")
)

type Bitcask struct {
	mu           sync.Mutex
	maxSize      int64 // MB
	dirPath      string
	keydir       *sync.Map
	activeFile   *bitcaskFile
	archiveFiles map[string]*bitcaskFile
}

func Open(dirPath string) (*Bitcask, error) {
	err := ensureDir(dirPath)
	if err != nil {
		return nil, err
	}

	// load data file
	activeFile, archiveFiles, err := loadArchiveFiles(dirPath)
	if err != nil {
		return nil, err
	}

	idxs, err := loadIndexFormFils(append(archiveFiles, activeFile)...)
	keydir := &sync.Map{}
	for k, v := range idxs {
		keydir.Store(k, v)
	}

	// load index
	if err != nil {
		return nil, err
	}

	archiveFileMap := make(map[string]*bitcaskFile)
	for _, archiveFile := range archiveFiles {
		archiveFileMap[archiveFile.fileId] = archiveFile
	}

	bitcask := &Bitcask{
		keydir:       keydir,
		dirPath:      dirPath,
		activeFile:   activeFile,
		archiveFiles: archiveFileMap,
		maxSize:      100 * 1024,
	}

	{
		// start merge task
		// ticker := time.NewTicker(time.Second * 10)
		// go func() {
		// 	for range ticker.C {
		// 		log.Printf("merge at %v", time.Now())
		// 		bitcask.Merge()
		// 	}
		// }()
	}
	return bitcask, nil
}

func (bc *Bitcask) Get(key string) (string, bool, error) {
	value, ok := bc.keydir.Load(key)
	if !ok {
		return "", false, nil
	}
	var result *Entry
	var err error

	val := value.(*EntryIndex)
	switch val.file_id {
	case bc.activeFile.fileId:
		result, err = bc.activeFile.Read(val)
		if err != nil {
			log.Println("bitcak error: ", err.Error())
			return "", false, err
		}
	default:
		result, err = bc.archiveFiles[val.file_id].Read(val)
		if err != nil {
			log.Println("bitcak error: ", err.Error())
			return "", false, err
		}
	}
	return string(result.value), true, nil
}

func (bc *Bitcask) Put(key, value string) error {
	if bc.activeFile.offset > bc.maxSize {
		// do slide active file
		err := bc.slideActive()
		if err != nil {
			return err
		}
	}
	idx, err := bc.activeFile.Write(NewEntry([]byte(key), []byte(value), PUT))
	if err != nil {
		log.Println("bitcak error: ", err.Error())
		return err
	}
	bc.keydir.Store(key, idx)
	return nil
}

func (bc *Bitcask) Delete(key string) error {
	// todo 考虑极端情况：删完缓存文件没删，程序挂了
	if _, ok := bc.keydir.LoadAndDelete(key); !ok {
		return nil
	}
	_, err := bc.activeFile.Write(NewEntry([]byte(key), []byte{}, DEL))
	if err != nil {
		log.Println("bitcak error: ", err.Error())
		return err
	}
	return nil
}
func (bc *Bitcask) List() []string {
	keys := make([]string, 0, 1024)
	bc.keydir.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys
}

// merge 定期压缩归档文件
// 1. 获取所有归档文件 （后期可以考虑只加载部分文件，但需要严格遵循时间关系）
// 2. 根据获取到的归档文件 获取所有索引信息
// 3. 仅当索引信息与内存中一致才写入一个新文件（归档文件中最大id+1024）
// 4. 完成后重命名文件（防止写入中途程序崩溃）
// 5. 删除老的数据
func (bc *Bitcask) Merge() error {
	archiveFiles := make([]*bitcaskFile, 0, len(bc.archiveFiles))
	fileIds := make([]string, 0, len(bc.archiveFiles))
	bc.mu.Lock()
	for fname, ar := range bc.archiveFiles {
		archiveFiles = append(archiveFiles, ar)
		fileIds = append(fileIds, strings.TrimSuffix(fname, _dataFileSuffix))
	}
	bc.mu.Unlock()

	// read all index
	// since the active file is up to date, you don’t need to worry about the deletion
	archiveIndexs, err := loadIndexFormFils(archiveFiles...)
	if err != nil {
		return err
	}

	newFileId, err := getMaxFileId(fileIds...)
	if err != nil {
		return GET_FILE_ID_ERROR
	}
	if newFileId == "" {
		// skip
		return nil
	}

	mergefile, err := OpenBitcaskFile(bc.dirPath+string(os.PathSeparator)+newFileId+_dataFileSuffix+_mergeFile, newFileId)
	if err != nil {
		return err
	}

	bc.archiveFiles[mergefile.fileId] = mergefile
	// update index
	for k, v := range archiveIndexs {
		ni, ok := bc.keydir.Load(k)
		if !ok {
			continue
		}
		nowIndex := ni.(*EntryIndex)
		if nowIndex.tstamp > v.tstamp || nowIndex.file_id != v.file_id {
			continue
		}
		oldFileId := nowIndex.file_id
		e, err := bc.archiveFiles[oldFileId].Read(v)
		if err != nil {
			return err
		}
		newIndex, err := mergefile.Write(e)
		if err != nil {
			return err
		}
		nowIndex.file_id = newIndex.file_id
		nowIndex.value_pos = newIndex.value_pos
	}

	bc.activeFile.Rename(strings.TrimSuffix(mergefile.path, _mergeFile))
	//delete old file
	bc.cleanFiles(archiveFiles...)
	return nil
}

// func (bc *bitcask) Fold() {
// }

func (bc *Bitcask) Sync() error {
	return bc.activeFile.Sync()
}

func (bc *Bitcask) Close() {
	if bc.activeFile != nil {
		bc.activeFile.Close()
	}
}

// 1. mv current active to archive
// 2. open a new active file
// should not happen at ths same time as Merge
func (bc *Bitcask) slideActive() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	defer bc.activeFile.Rename(strings.TrimSuffix(bc.activeFile.path, _activeFile))

	bc.archiveFiles[bc.activeFile.fileId] = bc.activeFile
	newActiveFileId := strconv.Itoa(int(time.Now().UnixNano()))
	newActiveFile, err := OpenBitcaskFile(bc.dirPath+string(os.PathSeparator)+newActiveFileId+_dataFileSuffix+_activeFile, newActiveFileId)
	if err != nil {
		return err
	}
	bc.activeFile = newActiveFile
	return nil
}

func loadArchiveFiles(dirPath string) (*bitcaskFile, []*bitcaskFile, error) {
	var activeFile *bitcaskFile
	archiveFiles := make([]*bitcaskFile, 0)
	fis, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, nil, err
	}

	for _, fi := range fis {
		if !fi.IsDir() {
			switch {
			case strings.HasSuffix(fi.Name(), _activeFile):
				if activeFile != nil {
					return nil, nil, FILE_ERROR
				}
				activeFile, err = OpenBitcaskFile(dirPath+string(os.PathSeparator)+fi.Name(), getFileId(fi.Name()))
				if err != nil {
					return nil, nil, err
				}
			case strings.HasSuffix(fi.Name(), _dataFileSuffix):
				archiveFile, err := OpenBitcaskFile(dirPath+string(os.PathSeparator)+fi.Name(), getFileId(fi.Name()))
				if err != nil {
					return nil, nil, err
				}
				archiveFiles = append(archiveFiles, archiveFile)
			}
		}

	}
	if activeFile == nil {
		activeFileId := strconv.Itoa(int(time.Now().UnixNano()))
		activeFile, err = OpenBitcaskFile(dirPath+string(os.PathSeparator)+activeFileId+_dataFileSuffix+_activeFile, activeFileId)
		if err != nil {
			return nil, nil, err
		}
	}
	return activeFile, archiveFiles, nil
}

// 获取所有索引，注意会去除删除的信息
func loadIndexFormFils(files ...*bitcaskFile) (map[string]*EntryIndex, error) {
	entryIndex := make(map[string]*EntryIndex)
	for _, file := range files {
		idx, err := file.loadIndexesFromFile()
		if err != nil {
			return nil, err
		}
		for k, nv := range idx {
			if ov, ok := entryIndex[k]; ok && ov.tstamp > nv.tstamp {
				continue
			}
			entryIndex[k] = nv
		}
	}

	for k, v := range entryIndex {
		if v.flag == DEL {
			delete(entryIndex, k)
		}
	}
	return entryIndex, nil
}

func (bc *Bitcask) cleanFiles(files ...*bitcaskFile) error {
	for _, file := range files {
		if err := file.Clean(); err != nil {
			return err
		}
		delete(bc.archiveFiles, bc.activeFile.fileId)
	}
	return nil
}

func getFileId(fileName string) string {
	var fileId string
	switch {
	case strings.HasSuffix(fileName, _activeFile):
		fileId = strings.TrimSuffix(fileName, _dataFileSuffix+_activeFile)
	case strings.HasSuffix(fileName, _dataFileSuffix):
		fileId = strings.TrimSuffix(fileName, _dataFileSuffix)
	}
	return fileId
}

func getMaxFileId(fileIds ...string) (string, error) {
	var maxFileId int64
	for _, fileId := range fileIds {
		fi, err := strconv.ParseInt(fileId, 10, 64)
		if err != nil {
			return "", err
		}
		if fi > maxFileId {
			maxFileId = fi
		}
	}
	if maxFileId == 0 {
		return "", nil
	}
	return strconv.Itoa(int(maxFileId) + 1024), nil
}

func ensureDir(path string) (err error) {
	if _, err = os.Stat(path); os.IsNotExist(err) {
		return os.Mkdir(path, 0755)
	}
	return
}
