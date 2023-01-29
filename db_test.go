package GoFlatDB

import (
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
		col, err := NewFlatDBCollection[TestData](dir, logger)
		require.NoError(t, err)

		res, err := col.Insert(&TestData{Foo: "hello world"})
		require.NoError(t, err)
		require.Equal(t, InsertResult{Id: 1}, res)
	})

	t.Run("Stress test", func(t *testing.T) {
		dir := t.TempDir()
		logger, err := zap.NewDevelopment()
		require.NoError(t, err)
		col, err := NewFlatDBCollection[TestData](dir, logger)
		require.NoError(t, err)

		for i := 0; i < 10_000; i++ {
			res, err := col.Insert(&TestData{Foo: "hello world"})
			require.NoError(t, err)
			require.Equal(t, InsertResult{Id: uint64(i) + 1}, res)
		}
	})
}

func BenchmarkFlatDBCollection(b *testing.B) {
	b.Run("Insert", func(b *testing.B) {
		dir := b.TempDir()
		logger, err := zap.NewDevelopment()
		require.NoError(b, err)
		col, err := NewFlatDBCollection[TestData](dir, logger)
		require.NoError(b, err)

		res, err := col.Insert(&TestData{Foo: "hello world"})
		require.NoError(b, err)
		require.Equal(b, InsertResult{Id: 1}, res)
	})

	b.Run("InsertInterface", func(b *testing.B) {
		dir := b.TempDir()
		logger, err := zap.NewDevelopment()
		require.NoError(b, err)
		col, err := NewFlatDBCollection[TestData](dir, logger)
		require.NoError(b, err)

		res, err := col.insertInterface(&TestData{Foo: "hello world"})
		require.NoError(b, err)
		require.Equal(b, InsertResult{Id: 1}, res)
	})
}
