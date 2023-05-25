package structure

import (
	"bufio"
	"github.com/alibaba/RedisShake/internal/log"
	"strconv"
	"strings"
)

func ReadNum(rd *bufio.Reader) int {
	line, err := rd.ReadString('\n')
	line = strings.TrimRight(line, "\r\n")
	if err != nil {
		log.PanicError(err)
	}
	num, _ := strconv.Atoi(line)
	return num
}
