package main

import (
	"io"
	"log"
)

type Page struct {
	buffer  []byte        // 缓存大小，20000行
	readPos int           // buffReadPos
	index   [][]int // 索引 hash(traceId) >> startIndex~endIndex
	errors  [][]byte
	isEmpty bool
}

func (p *Page) Read(body io.ReadCloser) int {
	if p.readPos >= pageLength {
		log.Fatalf("Buffer can not load 20000 rows data")
	}
	read, _ := body.Read(p.buffer[p.readPos:pageLength])
	if read == 0 {
		return -1
	}
	p.readPos += read
	return p.readPos
}

func (p *Page) CopyNext(next *Page, start int, end int) {
	tailLength := end - start
	copy(next.buffer[0:tailLength], p.buffer[start:end])
	next.readPos += tailLength
}

func (p *Page) Reset() {
	p.readPos = 0
	p.index = make([][]int, 0xffff)
	p.errors = make([][]byte, 0, 1024)
	p.isEmpty = true
}

func (p *Page) putSpans(traceId []byte, spanStart int, spanEnd int) {
	hash := p.hash(traceId)
	p.index[hash] = append(p.index[hash], spanStart, spanEnd)
	p.isEmpty = false
}

func (p *Page) putErrors(traceId []byte) {
	p.errors = append(p.errors, traceId)
}

func (p *Page) getSpans(traceId []byte) [][]byte {
	res := make([][]byte, 0, 128)
	hash := p.hash(traceId)
	spansIndex := p.index[hash]
	for i:=0; i < len(spansIndex); {
		res = append(res, p.buffer[spansIndex[i]:spansIndex[i+1]])
		i+=2
	}
	return res
}

func (p *Page) hash(traceId []byte) int {
	return (int(traceId[0]) +
		int(traceId[1])<<8 +
		int(traceId[2])<<16 +
		int(traceId[3])<<24) & 0xffff
}
