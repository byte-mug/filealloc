# filealloc
Malloc() but inside of a File. In Go.

## Traits

The `filealloc` implements a `malloc`-style Space-management on file level.
Unlike other similar packages that might be out there, `filealloc` is designed to be robust against loss-of-power or crashes **WITHOUT** the need for WAL logs of any kind.

- `filealloc` doesn't store pointers to file-locations in the file.
- `filealloc` uses bitmaps for block allocations, placed at deterministic fixed locations.

This ensures, that `filealloc` will not cease to function, even in case of file corruption of any sort.
`filealloc` keeps going no matter what!


# Example

```go
fmt.Println("Hello")
var cfg = filealloc.NewFormatConfig(12)

fobj,_ := os.OpenFile("testbin",os.O_RDWR|os.O_CREATE,0600)
alloc := &filealloc.PageAllocator{
	Storage : fobj,
	FormatConfig : cfg,
}
alloc.Init()

// allocate 17 contiguous blocks
first, ok, err := alloc.AllocateBlocks(17,true)
fmt.Println(first,ok,err)

alloc.Close()
```
