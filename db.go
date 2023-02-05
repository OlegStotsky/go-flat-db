package GoFlatDB

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"
)

type FlatDB struct {
	dir  string
	name string

	logger *zap.Logger
}

type InsertResult struct {
	ID uint64
}

type flatDBIndexUnorderedIndex struct {
	ordered   bool
	fieldName string

	data map[interface{}][]string // key - fieldName, val - fileName
}

type FlatDBCollection[T any] struct {
	name string
	dir  *os.File

	logger *zap.Logger

	mu               sync.RWMutex
	idFile           *os.File
	unorderedIndexes map[string]*flatDBIndexUnorderedIndex
}

func NewFlatDB(dir string, logger *zap.Logger) (*FlatDB, error) {
	name := filepath.Base(dir)

	dbLogger := logger.With(zap.String("db", name))

	return &FlatDB{
		name:   name,
		dir:    dir,
		logger: dbLogger,
	}, nil
}

type FlatDBCollectionOption[T any] func(db *FlatDBCollection[T])

func NewFlatDBCollection[T any](db *FlatDB, name string, logger *zap.Logger, opts ...FlatDBCollectionOption[T]) (*FlatDBCollection[T], error) {
	dir := filepath.Join(db.dir, name)

	idFilePath := filepath.Join(dir, "id.txt")
	if err := os.MkdirAll(dir, 0777); err != nil { // TODO: think about permissions
		return nil, errorCreatingFlatDBCollection(name, err)
	}
	// by this point we are sure that dir file exists
	dirFile, err := os.Open(dir)
	if err != nil {
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

	collectionLogger := logger.With(zap.String("collection", name))

	col := &FlatDBCollection[T]{
		name:             name,
		dir:              dirFile,
		logger:           collectionLogger,
		idFile:           idFile,
		unorderedIndexes: map[string]*flatDBIndexUnorderedIndex{},
	}

	for _, opt := range opts {
		opt(col)
	}

	return col, nil
}

func (c *FlatDBCollection[T]) Init() error {
	c.logger.Info("running init...")

	if len(c.unorderedIndexes) == 0 {
		return nil
	}

	files, err := os.ReadDir(c.dir.Name())
	if err != nil {
		return errorInitializingFlatDBCollection(c.dir.Name(), err)
	}

	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".json") {
			continue
		}

		docPath := documentFilePath(c.dir.Name(), f.Name())
		doc, err := c.readDocument(docPath)
		if err != nil {
			return errorInitializingFlatDBCollection(c.dir.Name(), err)
		}

		c.updateIndexes(doc)
	}

	return nil
}

func (c *FlatDBCollection[T]) QueryBuilder() *QueryBuilder[T] {
	return &QueryBuilder[T]{
		col: c,
		Q:   &NopQuery[T]{},
	}
}

func (c *FlatDBCollection[T]) updateIndexes(doc FlatDBModel[T]) {
	refVal := reflect.ValueOf(doc.Data)
	for _, index := range c.unorderedIndexes {
		fieldVal := refVal.FieldByName(index.fieldName)
		if !fieldVal.IsValid() {
			continue
		}

		fileName := documentFileName(doc.ID)

		c.mu.Lock()
		index.data[fieldVal.Interface()] = append(index.data[fieldVal.Interface()], fileName)
		c.mu.Unlock()
	}
}

func errorInitializingFlatDBCollection(name string, err error) error {
	return fmt.Errorf("error initializing FlatDBCollection %s: %w", name, err)
}

func errorCreatingFlatDBCollection(name string, err error) error {
	return fmt.Errorf("error creating FlatDBCollection %s: %w", name, err)
}

type FlatDBModel[T any] struct {
	Data T `json:"data"`

	ID uint64 `json:"ID"`
}

func (c *FlatDBCollection[T]) findBy(fieldName string, fieldValue interface{}) ([]FlatDBModel[T], error) {
	res := []FlatDBModel[T]{}
	{
		c.mu.RLock()
		defer c.mu.RUnlock()

		idx := c.unorderedIndexes[fieldName]
		if idx != nil {
			fileNames, ok := idx.data[fieldValue]
			if !ok {
				return []FlatDBModel[T]{}, DocumentNotFound
			}

			for _, docFileName := range fileNames {
				documentPath := documentFilePath(c.dir.Name(), docFileName)
				doc, err := c.readDocument(documentPath)
				if err != nil {
					return []FlatDBModel[T]{}, errorFindBy(fieldName, fieldValue, err)
				}
				res = append(res, doc)
			}

			return res, nil
		}
	}

	c.logger.Info("running full scan in findBy query", zap.String("fieldName", fieldName), zap.Any("fieldValue", fieldValue))

	files, err := os.ReadDir(c.dir.Name())
	if err != nil {
		return []FlatDBModel[T]{}, errorFindBy(fieldName, fieldValue, err)
	}

	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".json") {
			continue
		}

		doc, err := c.readDocument(documentFilePath(c.dir.Name(), f.Name()))
		if err != nil {
			return []FlatDBModel[T]{}, errorFindBy(fieldName, fieldValue, err)
		}

		val := reflect.ValueOf(doc.Data).FieldByName(fieldName)
		if !val.IsValid() {
			continue
		}

		if !reflect.DeepEqual(val.Interface(), fieldValue) {
			continue
		}

		res = append(res, doc)
	}

	return res, nil
}

func (c *FlatDBCollection[T]) findAll() ([]FlatDBModel[T], error) {
	res := []FlatDBModel[T]{}

	c.logger.Info("running full scan")

	files, err := os.ReadDir(c.dir.Name())
	if err != nil {
		return []FlatDBModel[T]{}, errorFindAll(err)
	}

	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".json") {
			continue
		}

		doc, err := c.readDocument(documentFilePath(c.dir.Name(), f.Name()))
		if err != nil {
			return []FlatDBModel[T]{}, errorFindAll(err)
		}

		res = append(res, doc)
	}

	return res, nil
}

func errorFindBy(fieldName string, val interface{}, err error) error {
	return fmt.Errorf("error findBy %s=%v: %w", fieldName, val, err)
}

func errorFindAll(err error) error {
	return fmt.Errorf("error findAll: %w", err)
}

func (c *FlatDBCollection[T]) GetByID(id uint64) (FlatDBModel[T], error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	doc, err := c.readDocument(documentFilePath(c.dir.Name(), documentFileName(id)))
	if err != nil {
		return FlatDBModel[T]{}, errorGettingDocumentByID(id, err)
	}

	return doc, nil
}

func (c *FlatDBCollection[T]) readDocument(documentPath string) (FlatDBModel[T], error) {
	var bytes []byte
	f, err := os.Open(documentPath)
	if err != nil {
		return FlatDBModel[T]{}, errorReadingDocument(documentPath, err)
	}
	defer func() {
		_ = f.Close()
	}()

	bufReader := bufio.NewReader(f)
	bytes, err = io.ReadAll(bufReader)
	if err != nil {
		return FlatDBModel[T]{}, errorReadingDocument(documentPath, err)
	}

	result := FlatDBModel[T]{}
	if err := json.Unmarshal(bytes, &result); err != nil {
		return result, errorReadingDocument(documentPath, err)
	}

	return result, nil
}

func errorReadingDocument(documentPath string, err error) error {
	return fmt.Errorf("error reading document %s: %w", documentPath, err)
}

func errorGettingDocumentByID(id uint64, err error) error {
	return fmt.Errorf("error getting document with id %d: %w", id, err)
}

func (c *FlatDBCollection[T]) Insert(data *T) (InsertResult, error) {
	id, err := c.GetNextID(c.idFile)
	if err != nil {
		return InsertResult{}, errInsertingIntoCollection(c.name, err)
	}

	model := FlatDBModel[T]{
		Data: *data,
		ID:   id,
	}
	bytes, err := json.Marshal(model)
	if err != nil {
		return InsertResult{}, errInsertingIntoCollection(c.name, err)
	}

	c.updateIndexes(model)

	return c.insertBytes(bytes, id)
}

func (c *FlatDBCollection[T]) insertBytes(data []byte, id uint64) (InsertResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fileName := documentFileName(id)

	docFilePath := documentFilePath(c.dir.Name(), fileName)

	f, err := os.Create(docFilePath) // TODO: think about permissions
	if err != nil {
		return InsertResult{}, errInsertingIntoCollection(c.name, err)
	}
	defer func() {
		_ = f.Close()
	}()

	bufWriter := bufio.NewWriter(f)
	_, err = bufWriter.Write(data)
	if err != nil {
		return InsertResult{}, errInsertingIntoCollection(c.name, err)
	}

	if err := bufWriter.Flush(); err != nil {
		return InsertResult{}, errInsertingIntoCollection(c.name, err)
	}

	return InsertResult{ID: id}, nil
}

func (c *FlatDBCollection[T]) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.idFile.Close(); err != nil {
		c.logger.Error("error closing id file", zap.Error(err))
	}

	if err := c.dir.Close(); err != nil {
		c.logger.Error("error closing dir file", zap.Error(err))
	}

	return nil
}

func documentFilePath(dir string, filename string) string {
	return filepath.Join(dir, filename)
}

func errInsertingIntoCollection(collection string, err error) error {
	return fmt.Errorf("error inserting into collection %s: %w", collection, err)
}

func documentFileName(id uint64) string {
	return strconv.FormatUint(id, 10) + ".json"
}

func (c *FlatDBCollection[T]) GetNextID(idFile *os.File) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

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
