package serializer

import (
	"bytes"
	"log"
)

const (
	WrongTrace = byte(1)
)

func PutType(buffer *[]byte, types byte) {
	*buffer = append(*buffer, types)
}

func Put(buffer *[]byte, traceId []byte, spans [][]byte) {
	length := len(traceId) + 1
	for i := range spans {
		length += len(spans[i]) + 1
	}
	length += 4

	*buffer = append(*buffer, uint8(length))
	*buffer = append(*buffer, uint8(length >> 8))
	*buffer = append(*buffer, uint8(length >> 16))
	*buffer = append(*buffer, uint8(length >> 24))

	*buffer = append(*buffer, traceId...)
	*buffer = append(*buffer, ',')

	for i := range spans {
		*buffer = append(*buffer, spans[i]...)
		*buffer = append(*buffer, ',')
	}
}

func GetWrongTrace(buffer []byte) map[string][]string {
	res := make(map[string][]string)
	if len(buffer) == 0 {
		return res
	}

	pos := 0
	types := buffer[pos]
	pos++
	if types != WrongTrace {
		log.Fatalf("Types %d != 1", types)
		return res
	}

	length := 1
	for pos < len(buffer) {
		length += int(buffer[pos]) + int(buffer[pos+1])<<8 + int(buffer[pos+2])<<16 + int(buffer[pos+3])<<24
		pos += 4

		traceId := ""
		for pos < length+1 {
			splitIndex := bytes.IndexByte(buffer[pos:length], ',')
			if splitIndex < 0 {
				break
			}
			if traceId == "" {
				traceId = string(buffer[pos : splitIndex+pos])
			} else {
				res[traceId] = append(res[traceId], string(buffer[pos:splitIndex+pos]))
			}
			pos += splitIndex + 1
		}
	}

	return res
}