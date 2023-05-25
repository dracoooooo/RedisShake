package aof

import (
	"bufio"
	"github.com/alibaba/RedisShake/internal/aof/structure"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/statistics"
	"io"
	"os"
	"time"
)

type Loader struct {
	filPath string
	fp      *os.File
	ch      chan *entry.Entry
}

func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	return ld
}

func (ld *Loader) ParseAOF() int {
	var err error
	ld.fp, err = os.OpenFile(ld.filPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicf("open file failed. file_path=[%s], error=[%s]", ld.filPath, err)
	}
	defer func() {
		err = ld.fp.Close()
		if err != nil {
			log.Panicf("close file failed. file_path=[%s], error=[%s]", ld.filPath, err)
		}
	}()

	rd := bufio.NewReader(ld.fp)

	// read entries
	ld.parseAOFEntry(rd)

	return 0
}

func (ld *Loader) parseAOFEntry(rd *bufio.Reader) {
	// for stat
	UpdateAOFSentSize := func() {
		offset, err := ld.fp.Seek(0, io.SeekCurrent)
		if err != nil {
			log.PanicError(err)
		}
		statistics.UpdateAOFSentSize(uint64(offset))
	}
	defer UpdateAOFSentSize()
	// read one entry
	tick := time.Tick(time.Second * 1)
	for true {
		// read *
		_, err := rd.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.PanicError(err)
			}
		}

		e := entry.NewEntry()
		argNum := structure.ReadNum(rd)
		for i := 0; i < argNum; i++ {
			// read $
			structure.ReadString(rd, 1)
			length := structure.ReadNum(rd)
			e.Argv = append(e.Argv, structure.ReadString(rd, length))
			// read \r\n
			structure.ReadString(rd, 2)
		}
		ld.ch <- e
		select {
		case <-tick:
			UpdateAOFSentSize()
		default:
		}
	}
}
