// bm_test.go: test suite for bmcompress
//
// Copyright (c) 2013 CloudFlare, Inc.

package bm

import (
	"bytes"
	"testing"
)

func assert(t *testing.T, b bool) {
	if !b {
		t.Fail()
	}
}

func TestCreateCompressor(t *testing.T) {
	b := new(bytes.Buffer)
	co := NewCompressor()
	co.SetWriter(b)
	d := new(Dictionary)
	assert(t, co.Ratio() == -1)
	assert(t, b != nil)
	assert(t, co != nil)
	assert(t, d != nil)
}

func TestSelfCompressAndExpand(t *testing.T) {
	b := new(bytes.Buffer)
	co := NewCompressor()
	co.SetWriter(b)
	d := new(Dictionary)
	assert(t, b != nil)
	assert(t, co != nil)
	assert(t, d != nil)
	s := []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog")
	dict := s
	d.Dict = dict
	co.SetDictionary(d)
	co.Write(s)
	assert(t, co.Ratio() == 0)
	co.Close()
	assert(t, b.Len() > 0)
	assert(t, b.Len() == 4)
	assert(t, b.Bytes()[0] == 0)
	assert(t, b.Bytes()[1] == 0)
	assert(t, b.Bytes()[2] == 0x81)
	assert(t, b.Bytes()[3] == 1)
	assert(t, co.Ratio() > 0)
	assert(t, co.Ratio() == (10000*b.Len()/len(s)))
	ex := NewExpander(b, dict)
	assert(t, ex != nil)
	out := make([]byte, 0)
	o, err := ex.Expand(out)
	assert(t, err == nil)
	assert(t, len(o) == len(s))
	assert(t, bytes.Compare(o, s) == 0)
}

func TestExtraAtEnd(t *testing.T) {
	b := new(bytes.Buffer)
	co := NewCompressor()
	co.SetWriter(b)
	d := new(Dictionary)
	assert(t, b != nil)
	assert(t, co != nil)
	assert(t, d != nil)
	s := []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog")
	dict := s
	d.Dict = dict
	co.SetDictionary(d)
	s = []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogDOG")
	co.Write(s)
	assert(t, co.Ratio() == 0)
	co.Close()
	assert(t, b.Len() > 0)
	assert(t, b.Len() == 8)
	assert(t, b.Bytes()[0] == 0)
	assert(t, b.Bytes()[1] == 0)
	assert(t, b.Bytes()[2] == 0x81)
	assert(t, b.Bytes()[3] == 1)
	assert(t, b.Bytes()[4] == 3)
	assert(t, co.Ratio() > 0)
	assert(t, co.Ratio() == (10000*b.Len()/len(s)))
	assert(t, bytes.Compare(b.Bytes()[5:8], []byte("DOG")) == 0)
	ex := NewExpander(b, dict)
	assert(t, ex != nil)
	out := make([]byte, 0)
	o, err := ex.Expand(out)
	assert(t, err == nil)
	assert(t, len(o) == len(s))
	assert(t, bytes.Compare(o, s) == 0)
}

func TestExtraAtStart(t *testing.T) {
	b := new(bytes.Buffer)
	co := NewCompressor()
	co.SetWriter(b)
	d := new(Dictionary)
	assert(t, b != nil)
	assert(t, co != nil)
	assert(t, d != nil)
	s := []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog")
	dict := s
	d.Dict = dict
	co.SetDictionary(d)
	s = []byte("THEthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog")
	co.Write(s)
	assert(t, co.Ratio() == 0)
	co.Close()
	assert(t, b.Len() > 0)
	assert(t, b.Len() == 8)
	assert(t, b.Bytes()[0] == 3)
	assert(t, bytes.Compare(b.Bytes()[1:4], []byte("THE")) == 0)
	assert(t, b.Bytes()[4] == 0)
	assert(t, b.Bytes()[5] == 0)
	assert(t, b.Bytes()[6] == 0x81)
	assert(t, b.Bytes()[7] == 1)
	assert(t, co.Ratio() > 0)
	assert(t, co.Ratio() == (10000*b.Len()/len(s)))
	ex := NewExpander(b, dict)
	assert(t, ex != nil)
	out := make([]byte, 0)
	o, err := ex.Expand(out)
	assert(t, err == nil)
	assert(t, len(o) == len(s))
	assert(t, bytes.Compare(o, s) == 0)
}

func TestExtraAtStartAndEnd(t *testing.T) {
	b := new(bytes.Buffer)
	co := NewCompressor()
	co.SetWriter(b)
	d := new(Dictionary)
	assert(t, b != nil)
	assert(t, co != nil)
	assert(t, d != nil)
	s := []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog")
	dict := s
	d.Dict = dict
	co.SetDictionary(d)
	s = []byte("THEthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogDOG")
	co.Write(s)
	assert(t, co.Ratio() == 0)
	co.Close()
	assert(t, b.Len() > 0)
	assert(t, b.Len() == 12)
	assert(t, b.Bytes()[0] == 3)
	assert(t, bytes.Compare(b.Bytes()[1:4], []byte("THE")) == 0)
	assert(t, b.Bytes()[4] == 0)
	assert(t, b.Bytes()[5] == 0)
	assert(t, b.Bytes()[6] == 0x81)
	assert(t, b.Bytes()[7] == 1)
	assert(t, b.Bytes()[8] == 3)
	assert(t, bytes.Compare(b.Bytes()[9:12], []byte("DOG")) == 0)
	assert(t, co.Ratio() > 0)
	assert(t, co.Ratio() == (10000*b.Len()/len(s)))
	ex := NewExpander(b, dict)
	assert(t, ex != nil)
	out := make([]byte, 0)
	o, err := ex.Expand(out)
	assert(t, err == nil)
	assert(t, len(o) == len(s))
	assert(t, bytes.Compare(o, s) == 0)
}

func TestNotQuite(t *testing.T) {
	b := new(bytes.Buffer)
	co := NewCompressor()
	co.SetWriter(b)
	d := new(Dictionary)
	assert(t, b != nil)
	assert(t, co != nil)
	assert(t, d != nil)
	s := []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog")
	dict := s
	d.Dict = dict
	co.SetDictionary(d)
	s = []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog the quick brown fox jumps over the lazy dog")
	co.Write(s)
	assert(t, co.Ratio() == 0)
	co.Close()
	assert(t, b.Len() > 0)
	assert(t, b.Len() == 48)
	assert(t, b.Bytes()[0] == 0)
	assert(t, b.Bytes()[1] == 0)
	assert(t, b.Bytes()[2] == 0x56)
	assert(t, b.Bytes()[3] == 0x2c)
	assert(t, bytes.Compare(b.Bytes()[4:], []byte(" the quick brown fox jumps over the lazy dog")) == 0)
	assert(t, co.Ratio() > 0)
	assert(t, co.Ratio() == (10000*b.Len()/len(s)))
	ex := NewExpander(b, dict)
	assert(t, ex != nil)
	out := make([]byte, 0)
	o, err := ex.Expand(out)
	assert(t, err == nil)
	assert(t, len(o) == len(s))
	assert(t, bytes.Compare(o, s) == 0)
}

func TestBitInTheMiddle(t *testing.T) {
	b := new(bytes.Buffer)
	co := NewCompressor()
	co.SetWriter(b)
	d := new(Dictionary)
	assert(t, b != nil)
	assert(t, co != nil)
	assert(t, d != nil)
	s := []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog")
	dict := s
	d.Dict = dict
	co.SetDictionary(d)
	s1 := "the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog"
	s1 = s1 + "HELLO JOHN" + s1
	s = []byte(s1)
	co.Write(s)
	assert(t, co.Ratio() == 0)
	co.Close()
	assert(t, b.Len() > 0)
	assert(t, b.Len() == 19)
	assert(t, b.Bytes()[0] == 0)
	assert(t, b.Bytes()[1] == 0)
	assert(t, b.Bytes()[2] == 0x81)
	assert(t, b.Bytes()[3] == 1)
	assert(t, b.Bytes()[4] == 10)
	assert(t, bytes.Compare(b.Bytes()[5:15], []byte("HELLO JOHN")) == 0)
	assert(t, b.Bytes()[15] == 0)
	assert(t, b.Bytes()[16] == 0)
	assert(t, b.Bytes()[17] == 0x81)
	assert(t, b.Bytes()[18] == 1)
	assert(t, co.Ratio() > 0)
	assert(t, co.Ratio() == (10000*b.Len()/len(s)))
	assert(t, b.Len() > 0)
	ex := NewExpander(b, dict)
	assert(t, ex != nil)
	out := make([]byte, 0)
	o, err := ex.Expand(out)
	assert(t, err == nil)
	assert(t, len(o) == len(s))
	assert(t, bytes.Compare(o, s) == 0)
}

func TestTotallyDifferent(t *testing.T) {
	b := new(bytes.Buffer)
	co := NewCompressor()
	co.SetWriter(b)
	d := new(Dictionary)
	assert(t, b != nil)
	assert(t, co != nil)
	assert(t, d != nil)
	s := []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog")
	dict := s
	d.Dict = dict
	co.SetDictionary(d)
	s = []byte("THE QUICK BROWN FOX JUMPS OVER THE LAZY DOGTHE QUICK BROWN FOX JUMPS OVER THE LAZY DOG THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG")
	co.Write(s)
	assert(t, co.Ratio() == 0)
	co.Close()
	assert(t, b.Len() > 0)
	assert(t, b.Len() == 132)
	assert(t, b.Bytes()[0] == 0x82)
	assert(t, b.Bytes()[1] == 0x01)
	assert(t, bytes.Compare(b.Bytes()[2:], []byte(s)) == 0)
	assert(t, co.Ratio() > 0)
	assert(t, co.Ratio() == (10000*b.Len()/len(s)))
	assert(t, b.Len() > 0)
	ex := NewExpander(b, dict)
	assert(t, ex != nil)
	out := make([]byte, 0)
	o, err := ex.Expand(out)
	assert(t, err == nil)
	assert(t, len(o) == len(s))
	assert(t, bytes.Compare(o, s) == 0)
}

func TestSerializeDeserialize(t *testing.T) {
	b := new(bytes.Buffer)
	co := NewCompressor()
	co.SetWriter(b)
	d := new(Dictionary)
	assert(t, b != nil)
	assert(t, co != nil)
	assert(t, d != nil)
	s := []byte("the quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dogthe quick brown fox jumps over the lazy dog")
	dict := s
	d.Dict = dict
	co.SetDictionary(d)
	s = []byte("THE QUICK BROWN FOX JUMPS OVER THE LAZY DOGTHE QUICK BROWN FOX JUMPS OVER THE LAZY DOG THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG")
	co.Write(s)
	assert(t, co.Ratio() == 0)
	co.Close()

	serialized, err := co.SerializeDictionary()
	assert(t, len(serialized) != 0)
	assert(t, err == nil)

	temp := make(map[uint32]uint32)
	for k, v := range co.GetDictionary().H {
		temp[k] = v
	}

	co.GetDictionary().H = make(map[uint32]uint32)
	assert(t, len(co.GetDictionary().H) == 0)

	m := make(map[uint32]uint32)
	err = DeserializeDictionary(serialized, m)
	assert(t, err == nil)

	assert(t, len(temp) != 0)
	assert(t, len(temp) == len(m))

	for k, v := range m {
		assert(t, temp[k] == v)
	}

	d.Dict = dict
	d.H = m
	co.SetDictionary(d)

	assert(t, len(temp) == len(co.GetDictionary().H))

	for k, v := range co.GetDictionary().H {
		assert(t, temp[k] == v)
	}
}
