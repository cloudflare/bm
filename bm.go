// bm.go: an implementation of the Bentley/McIlroy compression
// technique.  See "Data Compression Using Long Common Strings", Jon
// Bentley, Douglas McIlroy, Proceedings of the IEEE Data Compression
// Conference, 1999, pp. 287-295.
//
// Copyright (c) 2012-2013 CloudFlare, Inc.

package bm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// The compressor uses the Rabin/Karp algorithm to create fingerprints
// of sub-strings of the text to be compressed.
//
// Details of Rabin/Karp are in "Efficient randomized pattern-matching
// algorithms", Karp, R. M., and Rabin, M. O., IBM Journal of Research
// and Development 31, 2 March 1989, pp. 249-260.
//
// We work over bytes (rather than characters) and Rabin/Karp using
// arithmetic in base d in the ring Zp where p is prime and d is the
// nearest prime to the size of the input alphabet (since bytes d =
// 257).  For optimal storage we choose p such that pd fits as best as
// it can into 32 bits.  i.e. p is the prime nearest to
// 2^32/d. Various tricks are performed below to speed the computation
// of the hash. Notably p is not actually prime, it's a power of 2 so
// that & is used intead of %.
//
// Fingerprints are generated over a fixed block size which is defined
// here.  This is very open to experimentation and could actually be
// a parameter
//
// To make this as fast as possible the actual Rabin/Karp algorithm is
// not used, but values are picked to be powers of 2 so that slow
// operations can be made very fast.

const block uint32 = 50
const radix uint32 = (1 << 8) + 1
const prime uint32 = 1 << (32 - 8 - 1)
const clip uint32 = prime - 1 // Used to emulate a % operation when we
// know that prime is a power of two

// A Dictionary contains both the raw data being compressed against
// and the hash table built using the Rabin/Karp procedure
type Dictionary struct {
	Dict []byte // Bytes to compress against
	H    map[uint32]uint32
	// Stores the mapping between block checksums and their positions
}

// A Compressor is a complete instance of the compressor
type Compressor struct {
	w io.Writer // The io.Writer where compressed data will be written
	f uint32    // The current fingerprint as we are processing
	d []byte    // The data to be compressed.
	l uint32    // Largest 'digit' in the radix that will be seen in the
	// fingerprint
	save [256]uint32
	dict Dictionary

	// Values that keep track of the size of the data that was written
	// and the compressed output size

	inSize  int
	outSize int
}

// NewCompressor creates a new compressor.  The Compressor implements
// io.Writer and so calling Write() compress and writes to the actual
// output.  Note that you must call SetWriter and SetDictionary before
// doing any compression to set the output writer.
func NewCompressor() *Compressor {
	c := Compressor{}
	c.w = nil
	c.f = 0

	// Calculate the largest 'digit' that can be stored in the
	// fingerprint.  It's radix^(block-1) mod prime.  Calculated in a
	// loop to avoid an overflow when doing something like 256^100 mod
	// 16777213.

	c.l = 1
	var i uint32
	for i = 0; i < block-1; i++ {
		c.l *= radix
		c.l &= clip
	}

	for i = 0; i < 256; i++ {
		c.save[i] = i * c.l
	}

	c.inSize = 0
	c.outSize = 0

	return &c
}

// SetWriter sets the writer to which the compressed output will be written.
// This must be called otherwise an error will occur.
func (c *Compressor) SetWriter(w io.Writer) {
	c.w = w
}

// SetDictionary sets a dictionary. When a dictionary has been loaded
// references are made to the dictionary (rather than internally in
// the compressed data itself)
func (c *Compressor) SetDictionary(dict *Dictionary) {
	c.dict.Dict = dict.Dict

	// If the dictionary of hashes has not been computed then it must
	// be computed now
	if dict.H == nil {
		c.dict.H = make(map[uint32]uint32)

		f := uint32(0)
		for ii := range c.dict.Dict {
			i := uint32(ii)

			if i < block {
				f = (f*radix + uint32(c.dict.Dict[i])) & clip
			} else {
				if i%block == 0 {
					_, exists := c.dict.H[f]
					if !exists {
						c.dict.H[f] = uint32(i - block)
					}
				}

				f = (radix*(f-c.save[c.dict.Dict[i-block]]) +
					uint32(c.dict.Dict[i])) & clip
			}
		}
	} else {
		c.dict.H = dict.H
	}
}

// GetDictionary retrieves the dictionary structure for serialization
func (c *Compressor) GetDictionary() *Dictionary {
	return &c.dict
}

// Write implements the io.Writer interface.  To compress data Write
// repeatedly and it will be compressed.  When done it is necessary to
// call Close() where the actual compression occurs.
func (c *Compressor) Write(p []byte) (int, error) {
	c.d = append(c.d, p...)
	n := len(p)
	c.inSize += n
	return n, nil
}

// File format:
//
// A section of uncompressed data is written with a length value
// preceding it. The length is a base 128 number (a varint) with the
// same encoding Google Protocol Buffers uses.
//
// A compression section starts with a 0 (since no uncompressed
// section can have zero length) followed by a pair of varints giving
// the offset and length of the region to be copied.

// writeVarUInt: writes out a variable integer which used base 128
// in the style of Google Protocol Buffers.
func (c *Compressor) writeVarUint(u uint32) error {
	buf := make([]byte, 1)

	for {
		buf[0] = byte(u & 0x7F)
		u >>= 7

		if u != 0 {
			buf[0] |= 0x80
		}
		n, err := c.w.Write(buf)
		if err != nil {
			return err
		}
		c.outSize += n
		if u == 0 {
			break
		}
	}
	return nil
}

// writeUncompressedBlock: writes out a block of uncompressed data
// preceded by the length of the block as a variable length integer
func (c *Compressor) writeUncompressedBlock(d []byte) error {
	if len(d) == 0 {
		return nil
	}
	if err := c.writeVarUint(uint32(len(d))); err != nil {
		return err
	}
	if n, err := c.w.Write(d); err != nil {
		return err
	} else {
		c.outSize += n
	}
	return nil
}

// writeCompressedReference: writes out a block of compressed data
// which simply consists of a reference to the start of a block to
// copy and its length.  This is preceded by zero to indicate that
// this is a block of compressed data
func (c *Compressor) writeCompressedReference(start, offset uint32) error {
	zero := []byte{0}
	if n, err := c.w.Write(zero); err != nil {
		return err
	} else {
		c.outSize += n
	}
	if err := c.writeVarUint(start); err != nil {
		return err
	}

	return c.writeVarUint(offset)

}

// Close tells the compressor that all the data has been written.
// This does not close the underlying io.Writer.  This is where the
// Bentley/McIlroy and Rabin/Karp algorithms are implemented.
// Reference those papers for a full explanation.
func (c *Compressor) Close() error {
	var skip uint32
	var last uint32

	// This points to the slice containing the buffer used as the
	// dictionary for the compression.  This is either the data itself
	// (for self referential compression) or its the dictionary set by
	// SetDictionary

	for x := range c.d {
		i := uint32(x)

		// The first block bytes are consumed to calculate the
		// fingerprint of the first block

		if i < block {
			c.f = (c.f*radix + uint32(c.d[i])) & clip
		} else {

			// The data is broken up into non-overlapping blocks of
			// size block.  The fingerprints of each block are stored
			// in a hash table which keeps the position the first time
			// the block was seen.
			//
			// The fingerprint of the current block which covers the
			// block bytes (i-block,i] is calculated efficiently
			//
			// The canonical calculation is as follows:
			//
			// c.f = ( radix * ( c.f - c.d[i-block] * c.l ) + c.d[i] ) % prime
			//
			// But a number of tricks are performed to make this
			// faster. First, values of c.d[i-block] * c.l are kept in
			// an array so they are only calculated once. Second, the
			// modulo calculation is done using bit twiddling rather
			// than division.

			if i >= skip {

				// If the new fingerprint appears in the hash table
				// then this block has been seen before and can be
				// encoded rather than emitted.  First must check that
				// it's an actual match since there is a small
				// probability of the hashing algorithm used for
				// calculating fingerprints having a collision

				e, exists := c.dict.H[c.f]
				match := false
				if exists {
					match = true
					var j uint32
					for j = 0; j < block; j++ {
						if c.dict.Dict[e+j] != c.d[i-block+j] {
							match = false
							break
						}
					}
				}

				// If there's a match then we need to figure out how
				// far we can extend it backwards up to block-1 bytes
				// and forward as far as possible

				if match {
					var s uint32
					for s = 1; s < block; s++ {
						if i < last+block+s {
							break
						}

						if e < s {
							break
						}

						if i < block+s {
							break
						}

						if c.dict.Dict[e-s] != c.d[i-block-s] {
							break
						}
					}
					s--

					var f uint32
					for f = 0; f < uint32(len(c.d))-i; f++ {
						if e+block+f >= uint32(len(c.dict.Dict)) {
							break
						}

						if c.dict.Dict[e+block+f] != c.d[i+f] {
							break
						}
					}

					if err := c.writeUncompressedBlock(c.d[last : i-block-s]); err != nil {
						return err
					}
					if err := c.writeCompressedReference(e-s, block+s+f); err != nil {
						return err
					}
					skip = i + f + block + 1
					last = i + f
				}

			}

			c.f = ((c.f-c.save[c.d[i-block]])*radix + uint32(c.d[i])) & clip
		}
	}

	if last < uint32(len(c.d)) {
		return c.writeUncompressedBlock(c.d[last:])
	}

	return nil
}

// Ratio retrieves the compression ratio of the last compression
// performed. Only makes sense after Close() has been called. The
// returned value is an integer representing the size of the output as
// a percentage of the input * 100. If the return value is -1 then it
// indicates that there was no input.
func (c *Compressor) Ratio() int {
	if c.inSize > 0 {
		return (10000 * c.outSize) / c.inSize
	}
	return -1
}

// Get the size in bytes of the last compressed output. Only makes
// sense after Close() has been called.
func (c *Compressor) CompressedSize() int {
	return c.outSize
}

// Get the size in bytes of the last uncompressed input. Only makes
// sense after Close() has been called.
func (c *Compressor) InputSize() int {
	return c.inSize
}

// SerializeDictionary turns H (the map part of the Dictionary) into a
// []byte for easy storage in memcached or elsewhere.
func (c *Compressor) SerializeDictionary() ([]byte, error) {
	if len(c.dict.H) > 0 {

		// This reserves enough space in o to store the entire map
		// with the worse case encoding. The worse case encoding is
		// likely to be used for the map keys (because they are
		// hashes) but not for the offset values which will in general
		// be small.

		buf := bytes.NewBuffer(make([]byte, 0,
			len(c.dict.H)*2*binary.MaxVarintLen32))

		for k, v := range c.dict.H {
			if err := binary.Write(buf, binary.LittleEndian, k); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
				return nil, err
			}
		}

		return buf.Bytes(), nil
	}

	return []byte{}, nil
}

// DeserializeDictionary reads the H part of the Dictionary from a
// []byte previously created with SerializeDictionary
func DeserializeDictionary(o []byte, m map[uint32]uint32) error {
	buf := bytes.NewBuffer(o)

	for buf.Len() > 0 {
		var k uint32

		if err := binary.Read(buf, binary.LittleEndian, &k); err != nil {
			return err
		}
		var v uint32
		if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
			return err
		}
		m[k] = v
	}

	return nil
}

// An Expander is the complete state of the expander returned by NewExpander
type Expander struct {
	r io.Reader // The io.Reader from which the raw compressed data
	// is read
	d []byte // Data read from r is stored here so that
	// compressed back references can be handled
	to   int    // Position in d to which the caller has read
	dict []byte // Dictionary to decompress against, if set then
	// decompression is done referncing this.  If not
	// then references are internal.
}

// NewExpander creates a new decompressor.  Pass in an io.Reader that
// can be used to read the raw compressed data.  The Expander
// implements io.Reader and so calling Read() decompress data and
// reads the actual input.
func NewExpander(r io.Reader, dict []byte) *Expander {
	e := Expander{}
	e.r = r
	e.to = 0
	e.dict = dict
	return &e
}

// readVarUint: since the compressed data consists of varints (see
// bmcompress.go) for details then the fundamental operation is
// reading varints
func (e *Expander) readVarUint() (uint, error) {
	u := uint(0)
	b := make([]byte, 1)
	m := uint(1)
	for {
		if n, err := e.r.Read(b); n != 1 || err != nil {
			return 0, err
		}

		u += m * uint(b[0]&byte(0x7F))
		m *= 128

		if b[0] < 128 {
			break
		}
	}

	return u, nil
}

// Expand expands the compressed data into a buffer
func (e *Expander) Expand(p []byte) (q []byte, err error) {

	// This is done to capture the extreme case that an out of
	// bounds error occurs in the expansion. This should never
	// happen, but this protects against a corrupt compressed
	// block. If this occurs then we return no data at all to
	// prevent any bad data being returned to the client.

	defer func() {
		if x := recover(); x != nil {
			err = errors.New("panic caught inside expander")
			p = p[:0]
		}
	}()

	q = p
A:
	for {
		var u uint
		if u, err = e.readVarUint(); err != nil {
			break A
		}

		// If the value read is zero then it indicates a compressed
		// section which is formed of two varints indicating the
		// offset and length, if not then it's an uncompressed section

		if u == 0 {
			var offset uint
			if offset, err = e.readVarUint(); err != nil {
				break A
			}

			var length uint
			if length, err = e.readVarUint(); err != nil {
				break A
			}

			q = append(q, e.dict[offset:offset+length]...)
		} else {
			left := u
			for left > 0 {
				buf := make([]byte, left)
				var n int
				if n, err = e.r.Read(buf); err != nil {
					break A
				}
				if n > 0 {
					left -= uint(n)
					q = append(q, buf[0:n]...)
				} else {
					break A
				}
			}
		}
	}

	if err == io.EOF {
		err = nil
	}

	return
}
