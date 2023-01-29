package GoFlatDB

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFlatDBCollectionInsertBytes(t *testing.T) {
	t.Run("Simple insert works", func(t *testing.T) {
		dir := t.TempDir()
		logger, err := zap.NewDevelopment()
		require.NoError(t, err)
		col, err := NewFlatDBCollection(dir, logger)
		require.NoError(t, err)

		b := []byte(`{"hello": "world"}`)
		res, err := col.InsertBytes(b)
		require.NoError(t, err)
		require.Equal(t, InsertResult{Id: 1}, res)
	})

	t.Run("Stress test", func(t *testing.T) {
		dir := t.TempDir()
		logger, err := zap.NewDevelopment()
		require.NoError(t, err)
		col, err := NewFlatDBCollection(dir, logger)
		require.NoError(t, err)

		for i := 0; i < 10_000; i++ {
			b := []byte(`{"hello": "world"}`)
			res, err := col.InsertBytes(b)
			require.NoError(t, err)
			require.Equal(t, InsertResult{Id: uint64(i) + 1}, res)
		}
	})
}
