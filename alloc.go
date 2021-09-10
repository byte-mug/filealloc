// Copyright 2021 Simon Schmidt
// Licensed under the terms of the
// CC0 1.0 Universal license.

/*
Block based allocator for file-space-management.
*/
package filealloc

import (
	"io"
	"errors"
	"github.com/byte-mug/filealloc/bitmap"
)

// The existing chunks have been exthausted. Allocation impossible without growth.
var EXTHAUSTED = errors.New("EXTHAUSTED")

// Size exceeds the whole chunk size. There can't be so many contiguous blocks in the file.
var EXCEEDMAX = errors.New("EXCEEDMAX")

var outOfBounds = errors.New("OUT_OF_BOUNDS")

// A file. *os.File implements it.
type Storage interface{
	io.ReaderAt
	io.WriterAt
	io.Closer
	Sync() error
}

// A file MMAP interface to a file.
type MemMapper interface{
	MemmapAt(lng int, off int64) ([]byte,error)
	FlushMap(mm []byte) (error)
	MemUnmap(mm []byte)
}

func castMemMapper(s Storage) MemMapper {
	mm,_ := s.(MemMapper)
	return mm
}
var memMapperProviders = []func(Storage)MemMapper { castMemMapper }

func AddMemMapperProvider(f func(Storage)MemMapper) {
	memMapperProviders = append(memMapperProviders,f)
}
func getMemMapper(s Storage) MemMapper {
	for _,f := range memMapperProviders {
		mm := f(s)
		if mm!=nil { return mm }
	}
	return nil
}


type FormatConfig struct{
	// BlockSizeLog : log2 of the block size
	// BitmapBlocks : the size of the bitmaps in blocks
	// PrefixBlocks : the size of the file header in blocks
	BlockSizeLog, BitmapBlocks, PrefixBlocks uint8
	
	// If true, don't use mmap, not even if available.
	DontUseMmap bool
	
	// On mmapped areas: don't mem-sync
	DontMsync bool
	
	// On non-mmapped areas: don't fsync
	DontFsync bool
}
func (f *FormatConfig) BlockSize() int { return 1 << f.BlockSizeLog }
func (f *FormatConfig) RunSizeInBlocks() int64 { return int64(f.BitmapBlocks)<<(f.BlockSizeLog+3) }
func (f *FormatConfig) ChunkSizeInBlocks() int64 { return f.RunSizeInBlocks() + int64(f.BitmapBlocks) }
func (f *FormatConfig) BreakAddress(blk int64) (chunk, pos int64,ok bool) {
	blk -= int64(f.PrefixBlocks)
	if blk<0 { return }
	chunksiz := f.ChunkSizeInBlocks()
	chunk = blk/chunksiz
	pos = (blk%chunksiz) - int64(f.BitmapBlocks)
	ok = pos>=0
	return
}
func (f *FormatConfig) MakeAddress(chunk, pos int64) (blk int64) {
	blk = int64(f.PrefixBlocks)
	chunksiz := f.ChunkSizeInBlocks()
	blk += chunk*chunksiz
	blk += int64(f.BitmapBlocks)
	blk += pos
	return
}

// Creates a new FormatConfig with a block size of 1<<logBlockSize
func NewFormatConfig(logBlockSize uint8) FormatConfig {
	return FormatConfig{
		BlockSizeLog: logBlockSize,
		BitmapBlocks: 1,
		PrefixBlocks: 1,
	}
}

type bitmapBuffer struct{
	buffer  []byte
	rawoff  int64
	mmapped bool
}

// A page allocator.
type PageAllocator struct{
	Storage
	FormatConfig
	mmapper MemMapper
	bitmapSize int
	allocators []bitmapBuffer
}

// Initializes the page allocator after construction.
func (pa *PageAllocator) Init() {
	pa.bitmapSize = int(pa.BitmapBlocks)<<pa.BlockSizeLog
	if pa.DontUseMmap {
		pa.mmapper = nil
	} else {
		pa.mmapper = getMemMapper(pa.Storage)
	}
	buf := make([]byte,pa.bitmapSize)
	
	pos := int64(pa.PrefixBlocks)
	stride := pa.ChunkSizeInBlocks()
	
	i := 0
	for {
		n,_ := pa.ReadAt(buf,pos<<pa.BlockSizeLog)
		if n<=0 { break }
		i++
		pos += stride
	}
	
	if i==0 {
		for j := range buf { buf[j] = 0 }
		pa.WriteAt(buf,pos<<pa.BlockSizeLog)
		i++
	}
	
	pa.allocators = make([]bitmapBuffer,i)
	
	pos = int64(pa.PrefixBlocks)
	for j := range pa.allocators {
		pa.allocators[j] = pa.getAllocator(pos)
		pos += stride
	}
}

// Returns the number of chunks.
func (pa *PageAllocator) ChunksN() int { return len(pa.allocators) }

// Closes the allocator and the underlying file. Frees all associated resources.
func (pa *PageAllocator) Close() error {
	for i := range pa.allocators {
		if pa.allocators[i].mmapped {
			pa.mmapper.MemUnmap(pa.allocators[i].buffer)
			pa.allocators[i].buffer = nil
			pa.allocators[i].mmapped = false
		}
	}
	pa.allocators = nil
	pa.Storage.Close()
	return nil
}

func (pa *PageAllocator) getAllocator(off int64) (b bitmapBuffer) {
	b.rawoff = off<<pa.BlockSizeLog
	if pa.mmapper!=nil {
		buf,err := pa.mmapper.MemmapAt(pa.bitmapSize, b.rawoff)
		if err==nil && len(buf)>=pa.bitmapSize {
			b.buffer = buf
			b.mmapped = true
		}
	}
	if !b.mmapped {
		b.buffer = make([]byte,pa.bitmapSize)
		// Initial read.
		pa.ReadAt(b.buffer,b.rawoff)
	}
	return
}
func (pa *PageAllocator) appendAllocator() (err error) {
	var b bitmapBuffer
	off := pa.MakeAddress(int64(len(pa.allocators)),-int64(pa.BitmapBlocks))
	b.rawoff = off<<pa.BlockSizeLog
	b.buffer = make([]byte,pa.bitmapSize)
	_,err = pa.WriteAt(b.buffer,b.rawoff)
	if err!=nil { return }
	if pa.mmapper!=nil {
		buf,err2 := pa.mmapper.MemmapAt(pa.bitmapSize, b.rawoff)
		if err2==nil && len(buf)>=pa.bitmapSize {
			b.buffer = buf
			b.mmapped = true
		}
	}
	pa.allocators = append(pa.allocators,b)
	return
}

// msyncs the chunk's bitmap, if it is mmapped.
func (pa *PageAllocator) MemSyncIfMmapped(chunk int64) (err error, mmapped bool) {
	if int64(len(pa.allocators)) <= chunk { err = outOfBounds; return }
	if !pa.allocators[chunk].mmapped { return }
	mmapped = true
	err = pa.mmapper.FlushMap(pa.allocators[chunk].buffer)
	return
}

func (pa *PageAllocator) doAllocate(lng int64) (blk int64, ok bool,err error) {
	for i := range pa.allocators {
		blk,ok = bitmap.AllocateBitmap(pa.allocators[i].buffer,lng)
		if !ok { continue }
		blk = pa.MakeAddress(int64(i),blk)
		if !pa.allocators[i].mmapped {
			_,err = pa.WriteAt(pa.allocators[i].buffer,pa.allocators[i].rawoff)
			if !pa.DontFsync { pa.Sync() }
		} else if !pa.DontMsync {
			err = pa.mmapper.FlushMap(pa.allocators[i].buffer)
		}
		return
	}
	blk = 0
	err = EXTHAUSTED
	return
}

// Allocates a series of contiguous blocks.
// set grow = true, if the file should add a new chunk if needed.
func (pa *PageAllocator) AllocateBlocks(lng int64, grow bool) (blk int64, ok bool, err error) {
	if lng>pa.RunSizeInBlocks() {
		err = EXCEEDMAX
		return
	}
	for {
		blk,ok,err = pa.doAllocate(lng)
		if ok || err != EXTHAUSTED || !grow { return }
		err = pa.appendAllocator()
		if err!=nil { return }
	}
	panic("...")
}

func (pa *PageAllocator) doFree(blk int64, lng int64) (err error) {
	i, pos, ok := pa.BreakAddress(blk)
	if !ok { return }
	if int64(len(pa.allocators))>i {
		bitmap.FreeBitmap(pa.allocators[i].buffer,pos,lng)
		if !pa.allocators[i].mmapped {
			_, err = pa.WriteAt(pa.allocators[i].buffer,pa.allocators[i].rawoff)
			if !pa.DontFsync { pa.Sync() }
		} else if !pa.DontMsync {
			err = pa.mmapper.FlushMap(pa.allocators[i].buffer)
		}
	}
	return
}

// Free's a contiguous range of blocks.
func (pa *PageAllocator) FreeBlocks(blk int64, lng int64) (err error) {
	return pa.doFree(blk,lng)
}

