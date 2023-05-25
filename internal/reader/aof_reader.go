package reader

import (
	"github.com/alibaba/RedisShake/internal/aof"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/statistics"
	"os"
	"path/filepath"
)

type aofReader struct {
	path string
	ch   chan *entry.Entry
}

func NewAOFReader(path string) Reader {
	log.Infof("NewAOFReader: path=[%s]", path)
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		log.Panicf("NewAOFReader: filepath.Abs error: %s", err.Error())
	}
	log.Infof("NewAOFReader: absolute path=[%s]", absolutePath)
	r := new(aofReader)
	r.path = absolutePath
	return r
}

func (r *aofReader) StartRead() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)

	go func() {
		// start parse aof
		log.Infof("start send AOF. path=[%s]", r.path)
		fi, err := os.Stat(r.path)
		if err != nil {
			log.Panicf("NewAOFReader: os.Stat error: %s", err.Error())
		}
		statistics.Metrics.AofFileSize = uint64(fi.Size())
		statistics.Metrics.AofReceivedSize = uint64(fi.Size())
		aofLoader := aof.NewLoader(r.path, r.ch)
		_ = aofLoader.ParseAOF()
		log.Infof("send AOF finished. path=[%s]", r.path)
		close(r.ch)
	}()

	return r.ch
}
