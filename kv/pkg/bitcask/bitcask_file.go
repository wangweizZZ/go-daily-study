package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"time"
)

const (
	PUT uint16 = iota + 1
	DEL
)

type Entry struct {
	flag     uint16 // mark delete
	crc      uint32 // check file content
	tstamp   uint64 // entry create timestamp
	ksz      uint64 // key size
	value_sz uint64 // value size
	key      []byte // key
	value    []byte // value
}

type EntryIndex struct {
	file_id   string // this file id ,u
	flag      uint16 //
	value_sz  int64
	value_pos int64
	tstamp    uint64
}

type bitcaskFile struct {
	fileId string
	path   string
	file   *os.File
	offset int64
}

// entry
func NewEntry(key, value []byte, aciton uint16) *Entry {
	entry := &Entry{
		crc:      CRC32(value),
		ksz:      uint64(len(key)),
		value_sz: uint64(len(value)),
		key:      key,
		value:    value,
	}
	entry.tstamp = uint64(time.Now().UnixNano())
	return entry
}

func (e *Entry) Encode() ([]byte, error) {
	//flag + crc + tstamp + ksz + value_sz + key + value
	size := 2 + 4 + 8 + 8 + 8 + e.ksz + e.value_sz
	buf := make([]byte, size)
	binary.BigEndian.PutUint16(buf[0:2], e.flag)
	binary.BigEndian.PutUint32(buf[2:6], e.crc)
	binary.BigEndian.PutUint64(buf[6:14], e.tstamp)
	binary.BigEndian.PutUint64(buf[14:22], e.ksz)
	binary.BigEndian.PutUint64(buf[22:30], e.value_sz)
	copy(buf[30:(30+e.ksz)], e.key)
	copy(buf[(30+e.ksz):(30+e.ksz+e.value_sz)], e.value)
	return buf, nil
}

func Decode(buf []byte) (*Entry, error) {
	flag := binary.BigEndian.Uint16(buf[0:2])
	crc := binary.BigEndian.Uint32(buf[2:6])
	tstamp := binary.BigEndian.Uint64(buf[6:14])
	ksz := binary.BigEndian.Uint64(buf[14:22])
	value_sz := binary.BigEndian.Uint64(buf[22:30])
	key := buf[30:(30 + ksz)]
	value := buf[(30 + ksz):(30 + ksz + value_sz)]
	return &Entry{
		flag:     flag,
		crc:      crc,
		tstamp:   tstamp,
		ksz:      ksz,
		value_sz: value_sz,
		key:      key,
		value:    value,
	}, nil
}

func OpenBitcaskFile(filePath, id string) (*bitcaskFile, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	offset, err := file.Seek(0, 2)
	if err != nil {
		return nil, err
	}
	return &bitcaskFile{
		path:   filePath,
		file:   file,
		fileId: id,
		offset: offset,
	}, nil
}

func (bf *bitcaskFile) Read(idx *EntryIndex) (*Entry, error) {
	buf := make([]byte, idx.value_sz)
	_, err := bf.file.ReadAt(buf, idx.value_pos)
	if err != nil {
		return nil, err
	}
	return Decode(buf)
}

func (bf *bitcaskFile) Write(e *Entry) (*EntryIndex, error) {
	data, err := e.Encode()
	if err != nil {
		return nil, err
	}
	n, err := bf.file.WriteAt(data, bf.offset)
	if err != nil {
		return nil, err
	}
	bf.offset = bf.offset + int64(n)
	return &EntryIndex{
		file_id:   bf.fileId,
		value_pos: bf.offset - int64(n),
		value_sz:  int64(n),
		tstamp:    e.tstamp,
		flag:      e.flag,
	}, nil
}

func (bf *bitcaskFile) Sync() error {
	return bf.file.Sync()
}

func CRC32(bytes []byte) uint32 {
	return crc32.ChecksumIEEE(bytes)
}

func (bcf *bitcaskFile) Close() error {
	if bcf.file != nil {
		return bcf.file.Close()
	}
	return nil
}

func (bcf *bitcaskFile) Rename(newPath string) {
	if bcf.file != nil && bcf.path != "" {
		os.Rename(bcf.path, newPath)
	}
}

func (bf *bitcaskFile) loadIndexesFromFile() (map[string]*EntryIndex, error) {
	keydir := make(map[string]*EntryIndex)
	_, err := bf.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	var header_buf = make([]byte, 30)

	var offset int64
	for {
		_, err := bf.file.Read(header_buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		ksz := binary.BigEndian.Uint64(header_buf[14:22])
		value_sz := binary.BigEndian.Uint64(header_buf[22:30])

		key := make([]byte, ksz)
		_, err = bf.file.Read(key)
		if err != nil {
			return nil, err
		}
		keydir[string(key)] = &EntryIndex{
			file_id:   bf.fileId,
			flag:      binary.BigEndian.Uint16(header_buf[0:2]),
			value_sz:  int64(value_sz + ksz + 30),
			value_pos: offset,
			tstamp:    binary.BigEndian.Uint64(header_buf[6:14]),
		}
		offset, err = bf.file.Seek(int64(value_sz), 1)
		if err != nil {
			return nil, err
		}

		if offset >= bf.offset {
			break
		}
	}
	return keydir, nil
}

func (bf *bitcaskFile) Clean() error {
	if err := bf.Close(); err != nil {
		return err
	}
	return os.Remove(bf.path)
}
