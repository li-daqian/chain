package serializer

import (
	"log"
	"testing"
)

func TestWrongTrace(t *testing.T) {
	buffer := make([]byte, 1024)
	PutType(&buffer, WrongTrace)

	traceId := []byte("someTraceId")
	spans := make([][]byte, 10)
	spans = append(spans, []byte("span0"))
	spans = append(spans, []byte("span1"))
	Put(&buffer, traceId, spans)

	traceId = []byte("someTraceId1")
	spans = make([][]byte, 10)
	spans = append(spans, []byte("span0"))
	spans = append(spans, []byte("span1"))
	Put(&buffer, traceId, spans)

	traceId = []byte("someTraceId1")
	spans = make([][]byte, 10)
	spans = append(spans, []byte("span2"))
	spans = append(spans, []byte("span3"))
	Put(&buffer, traceId, spans)

	res := GetWrongTrace(buffer)
	log.Println(res)
}
