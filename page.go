package golite

import (
	"encoding/binary"
	"fmt"
)

const (
	// PageTypeInteriorIndex is a B-Tree interior index page.
	PageTypeInteriorIndex byte = 0x02
	// PageTypeInteriorTable is a B-Tree interior table page.
	PageTypeInteriorTable byte = 0x05
	// PageTypeLeafIndex is a B-Tree leaf index page.
	PageTypeLeafIndex byte = 0x0a
	// PageTypeLeafTable is a B-Tree leaf table page.
	PageTypeLeafTable byte = 0x0d
)

// LeafTableCell represents a cell in a leaf table page (type 0x0d).
// It contains the row's data and its unique identifier.
type LeafTableCell struct {
	PayloadSize int64
	RowID       int64
	Record      Record
}

// InteriorTableCell represents a cell in an interior table page (type 0x05).
// It points to a child page and contains a key for navigation.
type InteriorTableCell struct {
	LeftChildPageNum uint32
	Key              int64
}

// Page represents a single page from the SQLite database file.
type Page struct {
	Type          byte
	Freeblock     uint16
	CellCount     uint16
	CellContent   uint16
	Fragmented    byte
	RightMostPtr  uint32
	CellPointers  []uint16
	LeafCells     []LeafTableCell
	InteriorCells []InteriorTableCell
	RawData       []byte
}

// ParsePage reads a raw byte slice and parses it into a Page struct.
// pageNum is the 1-based page number, used to determine the header offset.
func ParsePage(data []byte, pageNum int) (*Page, error) {
	offset := 0
	if pageNum == 1 {
		offset = HeaderSize // The first page contains the 100-byte file header.
	}

	header := data[offset:]

	p := &Page{
		Type:        header[0],
		Freeblock:   binary.BigEndian.Uint16(header[1:3]),
		CellCount:   binary.BigEndian.Uint16(header[3:5]),
		CellContent: binary.BigEndian.Uint16(header[5:7]),
		Fragmented:  header[7],
		RawData:     data,
	}

	headerSize := 8
	// Interior pages have a 4-byte right-most pointer.
	if p.Type == PageTypeInteriorIndex || p.Type == PageTypeInteriorTable {
		headerSize = 12
		p.RightMostPtr = binary.BigEndian.Uint32(header[8:12])
	}

	// Parse the cell pointer array.
	p.CellPointers = make([]uint16, p.CellCount)
	cellPointerStart := offset + headerSize
	for i := 0; i < int(p.CellCount); i++ {
		pointerOffset := cellPointerStart + i*2
		p.CellPointers[i] = binary.BigEndian.Uint16(data[pointerOffset : pointerOffset+2])
	}

	// Parse the cells themselves based on the page type.
	switch p.Type {
	case PageTypeLeafTable:
		p.LeafCells = make([]LeafTableCell, p.CellCount)
		for i, cellOffset := range p.CellPointers {
			cellData := data[int(cellOffset):]
			payloadSize, n := readVarint(cellData)
			rowID, m := readVarint(cellData[n:])
			payloadOffset := n + m
			payload := cellData[payloadOffset : payloadOffset+int(payloadSize)]
			record, err := ParseRecord(payload)
			if err != nil {
				return nil, fmt.Errorf("failed to parse record in cell %d on page %d: %w", i, pageNum, err)
			}
			p.LeafCells[i] = LeafTableCell{
				PayloadSize: payloadSize,
				RowID:       rowID,
				Record:      record,
			}
		}
	case PageTypeInteriorTable:
		p.InteriorCells = make([]InteriorTableCell, p.CellCount)
		for i, cellOffset := range p.CellPointers {
			cellData := data[int(cellOffset):]
			leftChildPageNum := binary.BigEndian.Uint32(cellData[0:4])
			key, _ := readVarint(cellData[4:])

			p.InteriorCells[i] = InteriorTableCell{
				LeftChildPageNum: leftChildPageNum,
				Key:              key,
			}
		}
	case PageTypeLeafIndex, PageTypeInteriorIndex:
		// Index pages are not yet supported.
	}

	return p, nil
}

// readVarint reads a variable-length integer (varint) from the given byte slice.
// It returns the integer value and the number of bytes read.
func readVarint(data []byte) (int64, int) {
	var value int64
	var bytesRead int

	for i := 0; i < 9; i++ {
		if i >= len(data) {
			break
		}
		bytesRead++
		b := data[i]
		if i == 8 {
			value = (value << 8) | int64(b)
			break
		}
		value = (value << 7) | int64(b&0x7f)
		if b&0x80 == 0 {
			break
		}
	}
	return value, bytesRead
}
