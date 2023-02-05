package GoFlatDB

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type queryTestData struct {
	Foo string `json:"foo"`
	Bar string `json:"bar"`
}

func TestQuery(t *testing.T) {
	t.Run("simple test", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		col, err := NewFlatDBCollection[queryTestData](db, "test-collection", logger)
		require.NoError(t, err)

		d1 := &queryTestData{
			Foo: "hello",
			Bar: "5",
		}
		_, err = col.Insert(d1)
		require.NoError(t, err)

		d2 := &queryTestData{
			Foo: "hello",
			Bar: "world",
		}

		_, err = col.Insert(d2)
		require.NoError(t, err)

		{
			docs, err := col.
				QueryBuilder().
				Where("Foo", "=", "hello").
				And(col.QueryBuilder().Where("Bar", "=", "world")).
				Execute()
			require.NoError(t, err)

			require.Equal(t, 1, len(docs))
			require.Equal(t, uint64(2), docs[0].ID)
			require.Equal(t, *d2, docs[0].Data)
		}
		{
			docs, err := col.
				QueryBuilder().
				Where("Foo", "=", "hello").
				Execute()
			require.NoError(t, err)

			require.Equal(t, 2, len(docs))
		}
		{
			docs, err := col.
				QueryBuilder().
				Where("Bar", "=", "world").
				Execute()
			require.NoError(t, err)

			require.Equal(t, 1, len(docs))
			require.Equal(t, uint64(2), docs[0].ID)
			require.Equal(t, *d2, docs[0].Data)
		}
	})
}
