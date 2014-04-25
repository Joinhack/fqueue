package fqueue

import (
	"bufio"
	"os"
)

type Writer struct {
	*bufio.Writer
	fd *os.File
	*FQueue
}

func (b *Writer) rolling() error {
	if _, err := b.fd.Seek(MetaSize, os.SEEK_SET); err != nil {
		return err
	}
	b.WriterOffset = MetaSize
	return nil
}

func (b *Writer) setBottom() {
	if b.ReaderOffset < b.WriterOffset && b.fSize < b.Limit {
		b.WriterBottom = b.WriterOffset
	}
}

func (b *Writer) Close() error {
	if err := b.Flush(); err != nil {
		return err
	}
	return b.fd.Close()
}

func NewWriter(path string, q *FQueue) (w *Writer, err error) {
	var fd *os.File
	fd, err = os.OpenFile(path, os.O_WRONLY, 0644)
	if _, err = fd.Seek(int64(q.WriterOffset), os.SEEK_SET); err != nil {
		return
	}
	if err != nil {
		return
	}
	w = &Writer{
		FQueue: q,
		fd:     fd,
		Writer: bufio.NewWriter(fd),
	}
	err = nil
	return
}
