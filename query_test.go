package goflatdb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type queryTestData struct {
	Foo string `json:"foo"`
	Bar string `json:"bar"`
	Baz string `json:"baz"`
}

func TestQuery(t *testing.T) {
	t.Run("simple and query test", func(t *testing.T) {
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

	t.Run("and query properties test", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		col, err := NewFlatDBCollection[queryTestData](db, "test-collection", logger)
		require.NoError(t, err)

		for i := 0; i < 10000; i++ {
			foo := fmt.Sprintf("%d", rand.Intn(10))
			bar := fmt.Sprintf("%d", rand.Intn(10))
			baz := fmt.Sprintf("%d", rand.Intn(10))

			_, err := col.Insert(&queryTestData{
				foo,
				bar,
				baz,
			})
			require.NoError(t, err)
		}

		t.Run("a and b = b and a", func(t *testing.T) {
			docs1, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				And(col.QueryBuilder().Where("Bar", "=", "2")).
				Execute()
			require.NoError(t, err)

			docs2, err := col.
				QueryBuilder().
				Where("Bar", "=", "2").
				And(col.QueryBuilder().Where("Foo", "=", "1")).
				Execute()
			require.NoError(t, err)

			require.True(t, checkEqual(docs1, docs2))
		})

		t.Run("a and a = a", func(t *testing.T) {
			docs1, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				And(col.QueryBuilder().Where("Foo", "=", "1")).
				Execute()
			require.NoError(t, err)

			docs2, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				Execute()
			require.NoError(t, err)

			require.True(t, checkEqual(docs1, docs2))
		})

		t.Run("a and (b and c) = (a and b) and c", func(t *testing.T) {
			docs1, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				And(col.QueryBuilder().Where("Bar", "=", "2").
					And(col.QueryBuilder().Where("Baz", "=", "3"))).
				Execute()
			require.NoError(t, err)

			docs2, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				And(col.QueryBuilder().Where("Bar", "=", "2")).
				And(col.QueryBuilder().Where("Baz", "=", "3")).
				Execute()
			require.NoError(t, err)

			require.True(t, checkEqual(docs1, docs2))
		})
	})

	t.Run("simple or query test", func(t *testing.T) {
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

		d3 := &queryTestData{
			Foo: "foo",
			Bar: "world",
		}
		_, err = col.Insert(d3)
		require.NoError(t, err)

		d4 := &queryTestData{
			Foo: "qeew",
			Bar: "afssaf",
		}
		_, err = col.Insert(d4)
		require.NoError(t, err)

		{
			docs, err := col.
				QueryBuilder().
				Where("Foo", "=", "hello").
				Or(col.QueryBuilder().Where("Bar", "=", "world")).
				Execute()
			require.NoError(t, err)

			require.Equal(t, 3, len(docs))
			require.Equal(t, uint64(1), docs[0].ID)
			require.Equal(t, *d1, docs[0].Data)
			require.Equal(t, uint64(2), docs[1].ID)
			require.Equal(t, *d2, docs[1].Data)
			require.Equal(t, uint64(3), docs[2].ID)
			require.Equal(t, *d3, docs[2].Data)
		}
		{
			docs, err := col.
				QueryBuilder().
				Where("Foo", "=", "hello").
				Or(col.QueryBuilder().Where("Foo", "=", "hello")).
				Execute()
			require.NoError(t, err)

			require.Equal(t, 2, len(docs))
			require.Equal(t, uint64(1), docs[0].ID)
			require.Equal(t, *d1, docs[0].Data)
			require.Equal(t, uint64(2), docs[1].ID)
			require.Equal(t, *d2, docs[1].Data)
		}
	})

	t.Run("or query properties test", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		col, err := NewFlatDBCollection[queryTestData](db, "test-collection", logger)
		require.NoError(t, err)

		for i := 0; i < 10000; i++ {
			foo := fmt.Sprintf("%d", rand.Intn(10))
			bar := fmt.Sprintf("%d", rand.Intn(10))
			baz := fmt.Sprintf("%d", rand.Intn(10))

			_, err := col.Insert(&queryTestData{
				foo,
				bar,
				baz,
			})
			require.NoError(t, err)
		}

		t.Run("a or b = b or a", func(t *testing.T) {
			docs1, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				Or(col.QueryBuilder().Where("Bar", "=", "2")).
				Execute()
			require.NoError(t, err)

			docs2, err := col.
				QueryBuilder().
				Where("Bar", "=", "2").
				Or(col.QueryBuilder().Where("Foo", "=", "1")).
				Execute()
			require.NoError(t, err)

			require.True(t, checkEqual(docs1, docs2))
		})

		t.Run("a or a = a", func(t *testing.T) {
			docs1, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				Or(col.QueryBuilder().Where("Foo", "=", "1")).
				Execute()
			require.NoError(t, err)

			docs2, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				Execute()
			require.NoError(t, err)

			require.True(t, checkEqual(docs1, docs2))
		})

		t.Run("a or (b or c) = (a or b) or c", func(t *testing.T) {
			docs1, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				Or(col.QueryBuilder().Where("Bar", "=", "2").
					Or(col.QueryBuilder().Where("Baz", "=", "3"))).
				Execute()
			require.NoError(t, err)

			docs2, err := col.
				QueryBuilder().
				Where("Foo", "=", "1").
				Or(col.QueryBuilder().Where("Bar", "=", "2")).
				Or(col.QueryBuilder().Where("Baz", "=", "3")).
				Execute()
			require.NoError(t, err)

			require.True(t, checkEqual(docs1, docs2))
		})
	})

	t.Run("simple limit query test", func(t *testing.T) {
		dir := t.TempDir()

		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		db, err := NewFlatDB(dir, logger)
		require.NoError(t, err)

		col, err := NewFlatDBCollection[queryTestData](db, "test-collection", logger)
		require.NoError(t, err)

		for i := 0; i < 10000; i++ {
			foo := fmt.Sprintf("%d", rand.Intn(10))
			bar := fmt.Sprintf("%d", rand.Intn(10))
			baz := fmt.Sprintf("%d", rand.Intn(10))

			_, err := col.Insert(&queryTestData{
				foo,
				bar,
				baz,
			})
			require.NoError(t, err)
		}

		res, err := col.QueryBuilder().Select().Limit(100).Execute()
		require.NoError(t, err)

		require.Equal(t, 100, len(res))
	})
}

func checkEqual[T any](t1 []FlatDBModel[T], t2 []FlatDBModel[T]) bool {
	if len(t1) != len(t2) {
		return false
	}

	leftSet := map[uint64]struct{}{}
	for _, doc := range t1 {
		leftSet[doc.ID] = struct{}{}
	}

	rightSet := map[uint64]struct{}{}
	for _, doc := range t2 {
		rightSet[doc.ID] = struct{}{}
	}

	for id := range leftSet {
		_, ok := rightSet[id]
		if !ok {
			return false
		}
	}

	for id := range rightSet {
		_, ok := leftSet[id]
		if !ok {
			return false
		}
	}

	return true
}
