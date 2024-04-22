/*
   Copyright The Soci Snapshotter Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package compression

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	WindowSize = 32 * 1024
)

type Window [WindowSize]byte

// GzipZinfoGo is a go implementation of `GzipZinfoGo`.
type GzipZinfoGo struct {
	cZinfo      []byte
	checkpoints Offset
	spanSize    Offset
	list        map[SpanID]GzipZinfoCheckpoint
}

type GzipZinfoCheckpoint struct {
	out    Offset
	in     Offset
	bits   uint8
	window Window
}

func (i *GzipZinfoGo) AddCheckpoint(bits uint8, in, out Offset, window Window) {
	i.list[i.MaxSpanID()] = GzipZinfoCheckpoint{
		out:    out,
		in:     in,
		bits:   bits,
		window: window,
	}
}

// newGzipZinfo creates a new instance of `GzipZinfoGo` from cZinfo byte blob on zTOC.
func newGzipZinfoGo(zinfoBytes []byte) (*GzipZinfoGo, error) {
	if len(zinfoBytes) == 0 {
		return nil, fmt.Errorf("empty checkpoints")
	}

	b := bytes.NewReader(zinfoBytes)

	if len(zinfoBytes) < 12 {
		return nil, fmt.Errorf("invalid gzip zinfo")
	}

	cp := make([]byte, binary.Size(uint32(0)))
	_, err := b.Read(cp)
	if err != nil {
		return nil, fmt.Errorf("cannot read checkpoints")
	}
	checkpoints := binary.NativeEndian.Uint32(cp)

	ss := make([]byte, binary.Size(uint64(0)))
	_, err = b.Read(ss)
	if err != nil {
		return nil, fmt.Errorf("cannot read span size")
	}
	spanSize := binary.NativeEndian.Uint64(ss)

	if spanSize == 0 && checkpoints > 1 {
		return nil, fmt.Errorf("too few checkpoints")
	}

	return &GzipZinfoGo{
		cZinfo:      zinfoBytes,
		checkpoints: Offset(checkpoints),
		spanSize:    Offset(spanSize),
	}, nil
}

// newGzipZinfoFromFile creates a new instance of `GzipZinfoGo` given gzip file name and span size.
func newGzipZinfoFromFileGo(gzipFile string, spanSize int64) (*GzipZinfoGo, error) {
	var cZinfo []byte

	return &GzipZinfoGo{
		cZinfo: cZinfo,
	}, nil
}

// Close calls `C.free` on the pointer to `C.struct_gzip_zinfo`.
func (i *GzipZinfoGo) Close() {
	return
}

// Bytes returns the byte slice containing the zinfo.
func (i *GzipZinfoGo) Bytes() ([]byte, error) {
	return i.cZinfo, nil
}

// MaxSpanID returns the max span ID.
func (i *GzipZinfoGo) MaxSpanID() SpanID {
	if i == nil {
		return 0
	}
	return SpanID(i.checkpoints - 1)
}

// SpanSize returns the span size of the constructed ztoc.
func (i *GzipZinfoGo) SpanSize() Offset {
	return i.spanSize
}

// UncompressedOffsetToSpanID returns the ID of the span containing the data pointed by uncompressed offset.
func (i *GzipZinfoGo) UncompressedOffsetToSpanID(offset Offset) SpanID {
	/*
	   if (index == NULL)
	       return -1;

	   int res = 0;
	   struct gzip_checkpoint* here = index->list;
	   int ret = decode_int32(index->have);
	   while (--ret && decode_offset(here[1].out) <= off) {
	       here++;
	       res++;
	   }
	   return res;
	*/
	if i == nil {
		return -1
	}

	ss := i.spanSize
	for k, v := range i.list {
		if v.out <= offset && offset < v.out+ss {
			return k
		}
	}
	return -1
}

// ExtractDataFromBuffer wraps the call to `C.extract_data_from_buffer`, which takes in the compressed bytes
// and returns the decompressed bytes.
func (i *GzipZinfoGo) ExtractDataFromBuffer(compressedBuf []byte, uncompressedSize, uncompressedOffset Offset, spanID SpanID) ([]byte, error) {
	if len(compressedBuf) == 0 {
		return nil, fmt.Errorf("empty compressed buffer")
	}
	if uncompressedSize < 0 {
		return nil, fmt.Errorf("invalid uncompressed size: %d", uncompressedSize)
	}
	if uncompressedSize == 0 {
		return []byte{}, nil
	}
	b := make([]byte, uncompressedSize)

	r := bytes.NewReader(compressedBuf)

	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("could not create gzip reader: %v", err)
	}
	defer func(zr *gzip.Reader) {
		err := zr.Close()
		if err != nil {
			fmt.Printf("could not close gzip reader: %v", err)
		}
	}(zr)

	if _, err := zr.Read(b); err != nil && err != io.EOF {
		return nil, fmt.Errorf("could not read from gzip reader: %v", err)
	}

	return b, nil
}

// ExtractDataFromFile wraps `C.extract_data_from_file` and returns the decompressed bytes given the name of the .tar.gz file,
// offset and the size in uncompressed stream.
func (i *GzipZinfoGo) ExtractDataFromFile(fileName string, uncompressedSize, uncompressedOffset Offset) ([]byte, error) {
	if uncompressedSize < 0 {
		return nil, fmt.Errorf("invalid uncompressed size: %d", uncompressedSize)
	}
	if uncompressedSize == 0 {
		return []byte{}, nil
	}
	bytes := make([]byte, uncompressedSize)

	f, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %v", err)
	}

	if _, err := f.Read(bytes); err != nil {
		return nil, fmt.Errorf("could not read from file: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("could not close file: %v", err)
		}
	}()

	return i.ExtractDataFromBuffer(bytes, uncompressedSize, uncompressedOffset, 0)
}

// StartCompressedOffset returns the start offset of the span in the compressed stream.
func (i *GzipZinfoGo) StartCompressedOffset(spanID SpanID) Offset {
	start := i.getCompressedOffset(spanID)
	if i.hasBits(spanID) {
		start--
	}
	return start
}

// EndCompressedOffset returns the end offset of the span in the compressed stream. If
// it's the last span, returns the size of the compressed stream.
func (i *GzipZinfoGo) EndCompressedOffset(spanID SpanID, fileSize Offset) Offset {
	if spanID == i.MaxSpanID() {
		return fileSize
	}
	return i.getCompressedOffset(spanID + 1)
}

// StartUncompressedOffset returns the start offset of the span in the uncompressed stream.
func (i *GzipZinfoGo) StartUncompressedOffset(spanID SpanID) Offset {
	return i.getUncompressedOffset(spanID)
}

// EndUncompressedOffset returns the end offset of the span in the uncompressed stream. If
// it's the last span, returns the size of the uncompressed stream.
func (i *GzipZinfoGo) EndUncompressedOffset(spanID SpanID, fileSize Offset) Offset {
	if spanID == i.MaxSpanID() {
		return fileSize
	}
	return i.getUncompressedOffset(spanID + 1)
}

// VerifyHeader checks if the given zinfo has a proper header
func (i *GzipZinfoGo) VerifyHeader(r io.Reader) error {
	gz, err := gzip.NewReader(r)
	if gz != nil {
		err := gz.Close()
		if err != nil {
			return err
		}
	}
	return err
}

// getCompressedOffset wraps `C.get_comp_off` and returns the offset for the span in the compressed stream.
func (i *GzipZinfoGo) getCompressedOffset(spanID SpanID) Offset {
	return i.list[spanID].in
}

// hasBits wraps `C.has_bits` and returns true if any data is contained in the previous span.
func (i *GzipZinfoGo) hasBits(spanID SpanID) bool {
	if spanID >= SpanID(i.checkpoints) {
		return false
	}
	return i.list[spanID].bits != 0
}

// getUncompressedOffset wraps `C.get_uncomp_off` and returns the offset for the span in the uncompressed stream.
func (i *GzipZinfoGo) getUncompressedOffset(spanID SpanID) Offset {
	return i.list[spanID].out
}
