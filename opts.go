package GoFlatDB

func WithUnorderedIndex[T any](fieldName string) FlatDBCollectionOption[T] {
	return func(db *FlatDBCollection[T]) {
		db.unorderedIndexes[fieldName] = &flatDBIndexUnorderedIndex{
			ordered:   false,
			fieldName: fieldName,

			data: map[interface{}][]string{},
		}
	}
}
