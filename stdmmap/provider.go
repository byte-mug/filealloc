// Copyright 2021 Simon Schmidt
// Licensed under the terms of the
// CC0 1.0 Universal license.

// MemMapper implementation based on github.com/blevesearch/mmap-go
package stdmmap

import (
	"os"
	"github.com/blevesearch/mmap-go"
	"github.com/byte-mug/filealloc"
)



func WrapOsFile(s filealloc.Storage) filealloc.MemMapper {
	fobj,_ := s.(*os.File)
	if fobj==nil { return nil }
	return &file{fobj}
}

type file struct {
	f *os.File
}

func (f *file) MemmapAt(lng int, off int64) ([]byte, error) {
	buf,err := mmap.MapRegion(f.f,lng,mmap.RDWR,0,off)
	return []byte(buf),err
}

func (f *file) FlushMap(mm []byte) error {
	buf := mmap.MMap(mm)
	return buf.Flush()
}
func (f *file) MemUnmap(mm []byte) {
	buf := mmap.MMap(mm)
	buf.Unmap()
}

func init() {
	filealloc.AddMemMapperProvider(WrapOsFile)
}
