package goflatdb

import "fmt"

type QueryOperator uint8

const (
	OperatorEquals = iota
	OperatorLess
	OperatorMore
)

var operators = map[string]QueryOperator{
	"=": OperatorEquals,
	"<": OperatorLess,
	">": OperatorMore,
}

type Query[T any] interface {
	Execute() ([]FlatDBModel[T], error)
}

type QueryBuilder[T any] struct {
	col *FlatDBCollection[T]
	Q   Query[T]
}

func (c *QueryBuilder[T]) Where(fieldName string, operator string, fieldValue interface{}) *QueryBuilder[T] {
	whereQuery := &WhereQuery[T]{
		col:        c.col,
		fieldName:  fieldName,
		fieldValue: fieldValue,
	}

	op, err := parseOperator(operator)
	if err != nil {
		whereQuery.err = err

		c.Q = whereQuery
	}

	whereQuery.operator = op

	c.Q = whereQuery

	return c
}

func (c *QueryBuilder[T]) Select() *QueryBuilder[T] {
	selectQuery := SelectQuery[T]{
		col: c.col,
	}

	c.Q = &selectQuery

	return c
}

func (c *QueryBuilder[T]) And(q *QueryBuilder[T]) *QueryBuilder[T] {
	andQuery := AndQuery[T]{
		col: c.col,

		left:  c.Q,
		right: q.Q,
	}

	c.Q = &andQuery

	return c
}

func (c *QueryBuilder[T]) Or(q *QueryBuilder[T]) *QueryBuilder[T] {
	orQuery := OrQuery[T]{
		col: c.col,

		left:  c.Q,
		right: q.Q,
	}

	c.Q = &orQuery

	return c
}

func (c *QueryBuilder[T]) Limit(n int) *QueryBuilder[T] {
	limitQuery := LimitQuery[T]{
		col:   c.col,
		q:     c.Q,
		limit: n,
	}

	c.Q = &limitQuery

	return c
}

func (c *QueryBuilder[T]) Offset(n int) *QueryBuilder[T] {
	limitQuery := OffsetQuery[T]{
		col:    c.col,
		q:      c.Q,
		offset: n,
	}

	c.Q = &limitQuery

	return c
}

func (c *QueryBuilder[T]) Execute() ([]FlatDBModel[T], error) {
	return c.Q.Execute()
}

type NopQuery[T any] struct {
}

func (c *NopQuery[T]) Execute() ([]FlatDBModel[T], error) {
	return []FlatDBModel[T]{}, nil
}

type WhereQuery[T any] struct {
	col *FlatDBCollection[T]

	fieldName  string
	fieldValue interface{}
	operator   QueryOperator

	err error
}

func (c *WhereQuery[T]) Execute() ([]FlatDBModel[T], error) {
	if c.err != nil {
		return nil, fmt.Errorf("error executing where query: %w", c.err)
	}

	docs, err := c.col.findBy(c.fieldName, c.fieldValue)
	if err != nil {
		return nil, fmt.Errorf("error executing where query: %w", err)
	}

	return docs, nil
}

type AndQuery[T any] struct {
	col *FlatDBCollection[T]

	left  Query[T]
	right Query[T]

	err error
}

func (c *AndQuery[T]) Execute() ([]FlatDBModel[T], error) {
	leftResult, err := c.left.Execute()
	if err != nil {
		return nil, fmt.Errorf("error executing and query: %w", err)
	}

	rightResult, err := c.right.Execute()
	if err != nil {
		return nil, fmt.Errorf("error executing and query: %w", err)
	}

	leftSet := map[uint64]struct{}{}
	for _, doc := range leftResult {
		leftSet[doc.ID] = struct{}{}
	}

	result := []FlatDBModel[T]{}

	for _, doc := range rightResult {
		if _, ok := leftSet[doc.ID]; ok {
			result = append(result, doc)
		}
	}

	return result, nil
}

type OrQuery[T any] struct {
	col *FlatDBCollection[T]

	left  Query[T]
	right Query[T]

	err error
}

func (c *OrQuery[T]) Execute() ([]FlatDBModel[T], error) {
	leftResult, err := c.left.Execute()
	if err != nil {
		return nil, fmt.Errorf("error executing and query: %w", err)
	}

	rightResult, err := c.right.Execute()
	if err != nil {
		return nil, fmt.Errorf("error executing and query: %w", err)
	}

	leftSet := map[uint64]struct{}{}
	for _, doc := range leftResult {
		leftSet[doc.ID] = struct{}{}
	}

	result := []FlatDBModel[T]{}

	for _, doc := range leftResult {
		result = append(result, doc)
	}

	for _, doc := range rightResult {
		if _, ok := leftSet[doc.ID]; ok {
			continue
		}

		result = append(result, doc)
	}

	return result, nil
}

type LimitQuery[T any] struct {
	col *FlatDBCollection[T]

	q Query[T]

	limit int
}

func (c *LimitQuery[T]) Execute() ([]FlatDBModel[T], error) {
	docs, err := c.q.Execute()
	if err != nil {
		return nil, fmt.Errorf("error executing limit query: %w", err)
	}

	result := make([]FlatDBModel[T], 0, c.limit)

	n := c.limit
	if len(docs) < c.limit {
		n = len(result)
	}
	for i := 0; i < n; i++ {
		result = append(result, docs[i])
	}

	return result, nil
}

type OffsetQuery[T any] struct {
	col *FlatDBCollection[T]

	q Query[T]

	offset int
}

func (c *OffsetQuery[T]) Execute() ([]FlatDBModel[T], error) {
	docs, err := c.q.Execute()
	if err != nil {
		return nil, fmt.Errorf("error executing limit query: %w", err)
	}

	if len(docs) == 0 || c.offset >= len(docs) {
		return []FlatDBModel[T]{}, nil
	}

	return docs[c.offset:], nil
}

type SelectQuery[T any] struct {
	col *FlatDBCollection[T]
}

func (c *SelectQuery[T]) Execute() ([]FlatDBModel[T], error) {
	docs, err := c.col.findAll()
	if err != nil {
		return nil, fmt.Errorf("error executing select query: %w", err)
	}

	return docs, nil
}

func parseOperator(op string) (QueryOperator, error) {
	qOp, ok := operators[op]
	if !ok {
		return QueryOperator(0), fmt.Errorf("error parsing operator %s", op)
	}

	return qOp, nil
}
