package structure

import (
	"bufio"
	"github.com/alibaba/RedisShake/internal/log"
	"io"
)

func ReadString(rd *bufio.Reader, n int) string {
	buf := make([]byte, n)
	_, err := io.ReadFull(rd, buf)
	if err != nil {
		log.PanicError(err)
	}
	return string(buf)
}
