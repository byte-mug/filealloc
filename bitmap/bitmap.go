// Copyright 2021 Simon Schmidt
// Licensed under the terms of the
// CC0 1.0 Universal license.

/*
Implements a bitmap for block allocation (or similar things).
A 0-bit denotes a Free block, a 1-bit denotes a Occupied block.
The first bit of a byte is assumed to be the MSB.
The last bit of a byte is assumed to be the LSB.
*/
package bitmap


func findFreeSpot8(bm []byte, lng uint) (pos int64,ok bool) {
	B := byte(0xff<<(8-lng))
	
	for j,c := range bm {
		b := B
		i := uint(8)
		for ; i>0; i-- {
			if (c & b)==0 {
				break
			}
			b>>=1
		}
		if i>=lng {
			return int64(j<<3) | int64(8-i) , true
		} else if i>0 && j<len(bm)-1 {
			b = B
			b <<= (8-i)
			c = bm[j+1]
			if (c & b)==0 {
				return int64(j<<3) | int64(8-i) , true
			}
		}
	}
	
	return
}
func matchAligned(bm []byte, bipos int64, lng int64) bool {
	n := lng>>3;
	l := int64(len(bm))
	if bipos+n > l { return false }
	for j := n; j>0; j-- {
		if bm[bipos]!=0 { return false }
		bipos++
	}
	m := lng&7
	if m==0 { return true }
	b := byte(0xff<<uint(8-m))
	if bipos >= l { return false }
	if (bm[bipos] & b)==0 { return true }
	return false
}

// Finds a range of free slots inside of a bitmap.
func FindFreeSpot(bm []byte, lng int64) (int64,bool) {
	if lng<0 { panic("illegal arg") }
	if lng<=8 {
		return findFreeSpot8(bm,uint(lng))
	}
	B := byte(0xff)
	for j,c := range bm {
		b := B
		i := uint(8)
		for ; i>0; i-- {
			if (c & b)==0 {
				break
			}
			b>>=1
		}
		if i==0 { continue }
		if matchAligned(bm,int64(j+1),lng-int64(i)) {
			return int64(j<<3) | int64(8-i) , true
		}
	}
	return 0,false
}

// Allocates a range of slots inside of a bitmap.
// panics if pos+len > len(bm)*8
func WriteInUse(bm []byte, pos, lng int64) {
	if pos<0 || lng<0 { panic("illegal arg") }
	n := pos&7
	if ((lng+n)>>3) == 0 {
		b := byte(0xff)
		b <<= uint(8-lng)
		b >>= uint(n)
		bm[pos>>3] |= b
		return
	}
	if n!=0 {
		b := byte(0xff)
		b >>= uint(n)
		bm[pos>>3] |= b
		pos += 8-n
		lng -= 8-n
	}
	for lng>=8 {
		bm[pos>>3] = 0xff
		pos += 8
		lng -= 8
	}
	if lng>0 {
		b := byte(0xff)
		b <<= uint(8-lng)
		bm[pos>>3] |= b
	}
}

// Frees a range of slots inside of a bitmap.
// panics if pos+len > len(bm)*8
func WriteFree(bm []byte, pos, lng int64) {
	if pos<0 || lng<0 { panic("illegal arg") }
	n := pos&7
	if ((lng+n)>>3) == 0 {
		b := byte(0xff)
		b <<= uint(8-lng)
		b >>= uint(n)
		bm[pos>>3] &= ^b
		return
	}
	if n!=0 {
		b := byte(0xff)
		b >>= uint(n)
		bm[pos>>3] &= ^b
		pos += 8-n
		lng -= 8-n
	}
	for lng>=8 {
		bm[pos>>3] = 0
		pos += 8
		lng -= 8
	}
	if lng>0 {
		b := byte(0xff)
		b <<= uint(8-lng)
		bm[pos>>3] &= ^b
	}
}

// Finds and allocates a range of free blocks inside of a bitmap.
func AllocateBitmap(bm []byte, lng int64) (int64, bool) {
	pos,ok := FindFreeSpot(bm,lng)
	if ok { WriteInUse(bm,pos,lng) }
	return pos,ok
}

// Frees a range of slots inside of a bitmap.
func FreeBitmap(bm []byte, pos, lng int64) {
	max := int64(len(bm)*8)-pos
	if max<lng { lng = max }
	if lng > 0 { WriteFree(bm,pos,lng) }
}

