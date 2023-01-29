package GoFlatDB

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"go.uber.org/zap"
)

type FlatDB struct {
	dbDir string

	logger *zap.Logger
}

type InsertResult struct {
	Id uint64
}

type FlatDBCollection struct {
	name string
	dir  string

	logger *zap.Logger

	idFile *os.File
}

func NewFlatDBCollection(dir string, logger *zap.Logger) (*FlatDBCollection, error) {
	name := filepath.Base(dir)

	idFilePath := filepath.Join(dir, "id.txt")
	if err := os.MkdirAll(dir, 777); err != nil { // TODO: think about permissions
		return nil, errorCreatingFlatDBCollection(name, err)
	}
	idFile, err := os.Open(idFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			idFile, err = os.Create(idFilePath)
			if err != nil {
				return nil, errorCreatingFlatDBCollection(name, err)
			}
			if err := writeID(idFile, 0); err != nil {
				return nil, errorCreatingFlatDBCollection(name, err)
			}
		} else {
			return nil, errorCreatingFlatDBCollection(name, err)
		}
	}

	return &FlatDBCollection{
		name:   name,
		dir:    dir,
		logger: logger,
		idFile: idFile,
	}, nil
}

func errorCreatingFlatDBCollection(name string, err error) error {
	return fmt.Errorf("error creating FlatDBCollection %s: %w", name, err)
}

func (c *FlatDBCollection) InsertBytes(data []byte) (InsertResult, error) {
	id, err := GetNextID(c.idFile)
	if err != nil {
		return InsertResult{}, errInsertingIntoCollection(c.name, err)
	}

	fileName := documentFileName(id)

	docFilePath := filepath.Join(c.dir, fileName)

	f, err := os.Create(docFilePath) // TODO: think about permissions
	if err != nil {
		return InsertResult{}, errInsertingIntoCollection(c.name, err)
	}
	defer f.Close()

	bufWriter := bufio.NewWriter(f)
	_, err = bufWriter.Write(data)
	if err != nil {
		return InsertResult{}, errInsertingIntoCollection(c.name, err)
	}

	if err := bufWriter.Flush(); err != nil {
		return InsertResult{}, errInsertingIntoCollection(c.name, err)
	}

	return InsertResult{Id: id}, nil
}

func errInsertingIntoCollection(collection string, err error) error {
	return fmt.Errorf("error inserting into collection %s: %w", collection, err)
}

func documentFileName(id uint64) string {
	return strconv.FormatUint(id, 10) + ".json"
}

func GetNextID(idFile *os.File) (uint64, error) {
	curID, err := readID(idFile)
	if err != nil {
		return 0, fmt.Errorf("error generating next id: %w", err)
	}

	nextID := curID + 1
	if err := writeID(idFile, nextID); err != nil {
		return 0, fmt.Errorf("error generating next id: %w", err)
	}

	return nextID, nil
}

func readID(r io.ReadSeeker) (uint64, error) {
	id, err := readUInt64(r)
	if err != nil {
		return 0, err
	}

	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("error reading id: %w", err)
	}

	return id, nil
}

func writeID(w io.WriteSeeker, id uint64) error {
	err := writeUInt64(w, id)
	if err != nil {
		return err
	}

	_, err = w.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("error writing id: %w", err)
	}

	return nil
}

func readUInt64(r io.Reader) (uint64, error) {
	var bytes [8]byte
	_, err := r.Read(bytes[:])
	if err != nil {
		return 0, fmt.Errorf("error reading uint64: %w", err)
	}

	return binary.BigEndian.Uint64(bytes[:]), nil
}

func writeUInt64(w io.Writer, n uint64) error {
	var bytes [8]byte
	binary.BigEndian.PutUint64(bytes[:], n)

	_, err := w.Write(bytes[:])
	if err != nil {
		return fmt.Errorf("error writing uint64: %w", err)
	}

	return nil
}
