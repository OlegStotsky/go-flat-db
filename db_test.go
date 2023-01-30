package GoFlatDB

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type TestData struct {
	Foo string `json:"foo"`
}

func TestFlatDBCollectionInsertBytes(t *testing.T) {
	t.Run("Simple insert works", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		col, err := NewFlatDBCollection[TestData](db, "test-collection", logger)
		require.NoError(t, err)

		res, err := col.Insert(&TestData{Foo: "hello world"})
		require.NoError(t, err)
		require.Equal(t, InsertResult{ID: 1}, res)
	})

	t.Run("Inserting of multiple collections of one db works", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		{
			col, err := NewFlatDBCollection[TestData](db, "test-collection", logger)
			require.NoError(t, err)

			res, err := col.Insert(&TestData{Foo: "hello world"})
			require.NoError(t, err)
			require.Equal(t, InsertResult{ID: 1}, res)
		}

		{
			col, err := NewFlatDBCollection[TestData](db, "test-collection2", logger)
			require.NoError(t, err)

			res, err := col.Insert(&TestData{Foo: "hello world"})
			require.NoError(t, err)
			require.Equal(t, InsertResult{ID: 1}, res)
		}
	})

	t.Run("Stress test", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		col, err := NewFlatDBCollection[TestData](db, "test-collection", logger)
		require.NoError(t, err)

		col2, err := NewFlatDBCollection[TestData](db, "test-collection2", logger)
		require.NoError(t, err)

		for i := 0; i < 10_000; i++ {
			{
				res, err := col.Insert(&TestData{Foo: "hello world"})
				require.NoError(t, err)
				require.Equal(t, InsertResult{ID: uint64(i) + 1}, res)
			}

			{
				res, err := col2.Insert(&TestData{Foo: "hello world"})
				require.NoError(t, err)
				require.Equal(t, InsertResult{ID: uint64(i) + 1}, res)
			}
		}
	})
}

func TestFlatDBCollectionGetByID(t *testing.T) {
	t.Run("Simple GetByID works", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		col, err := NewFlatDBCollection[TestData](db, "test-collection", logger)
		require.NoError(t, err)

		data := TestData{Foo: "hello world"}
		{
			res, err := col.Insert(&data)
			require.NoError(t, err)
			require.Equal(t, InsertResult{ID: 1}, res)
		}
		{
			res, err := col.GetByID(1)
			require.NoError(t, err)

			require.Equal(t, data, res.Data)
			require.Equal(t, uint64(1), res.ID)
		}
	})

	t.Run("GetByID works with multiple collections", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		{
			col, err := NewFlatDBCollection[TestData](db, "test-collection", logger)
			require.NoError(t, err)

			data := TestData{Foo: "hello world"}
			{
				res, err := col.Insert(&data)
				require.NoError(t, err)
				require.Equal(t, InsertResult{ID: 1}, res)
			}
			{
				res, err := col.GetByID(1)
				require.NoError(t, err)

				require.Equal(t, data, res.Data)
				require.Equal(t, uint64(1), res.ID)
			}
		}

		{
			col, err := NewFlatDBCollection[TestData](db, "test-collection2", logger)
			require.NoError(t, err)

			data := TestData{Foo: "hello world"}
			{
				res, err := col.Insert(&data)
				require.NoError(t, err)
				require.Equal(t, InsertResult{ID: 1}, res)
			}
			{
				res, err := col.GetByID(1)
				require.NoError(t, err)

				require.Equal(t, data, res.Data)
				require.Equal(t, uint64(1), res.ID)
			}
		}
	})

	t.Run("Stress test", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		col, err := NewFlatDBCollection[TestData](db, "test-collection", logger)
		require.NoError(t, err)

		col2, err := NewFlatDBCollection[TestData](db, "test-collection2", logger)
		require.NoError(t, err)

		for i := 0; i < 10_000; i++ {
			testData := TestData{Foo: strconv.FormatInt(rand.Int63(), 10)}
			{
				res, err := col.Insert(&testData)
				require.NoError(t, err)
				require.Equal(t, InsertResult{ID: uint64(i) + 1}, res)
			}
			{
				res, err := col.GetByID(uint64(i) + 1)
				require.NoError(t, err)
				require.Equal(t, testData, res.Data)
				require.Equal(t, uint64(i)+1, res.ID)
			}
			{
				res, err := col2.Insert(&testData)
				require.NoError(t, err)
				require.Equal(t, InsertResult{ID: uint64(i) + 1}, res)
			}
			{
				res, err := col2.GetByID(uint64(i) + 1)
				require.NoError(t, err)
				require.Equal(t, testData, res.Data)
				require.Equal(t, uint64(i)+1, res.ID)
			}
		}
	})

	t.Run("Stress test", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		col, err := NewFlatDBCollection[TestData](db, "test-collection", logger)
		require.NoError(t, err)

		col2, err := NewFlatDBCollection[TestData](db, "test-collection2", logger)
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			i := i
			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				t.Parallel()
				testData := TestData{Foo: strconv.FormatInt(rand.Int63(), 10)}
				{
					insertResult, err := col.Insert(&testData)
					require.NoError(t, err)
					res, err := col.GetByID(insertResult.ID)
					require.NoError(t, err)
					require.Equal(t, testData, res.Data)
					require.Equal(t, insertResult.ID, res.ID)
				}
				{
					insertResult, err := col2.Insert(&testData)
					require.NoError(t, err)
					res, err := col2.GetByID(insertResult.ID)
					require.NoError(t, err)
					require.Equal(t, testData, res.Data)
					require.Equal(t, insertResult.ID, res.ID)
				}
			})
		}
	})
}

func BenchmarkFlatDBCollection(b *testing.B) {
	b.Run("Insert", func(b *testing.B) {
		dir := b.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(b, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(b, err)

		col, err := NewFlatDBCollection[TestData](db, "test-collection", logger)
		require.NoError(b, err)

		res, err := col.Insert(&TestData{Foo: "hello world"})
		require.NoError(b, err)
		require.Equal(b, InsertResult{ID: 1}, res)
	})
}
