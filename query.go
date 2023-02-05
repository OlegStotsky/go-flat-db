package GoFlatDB

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

func parseOperator(op string) (QueryOperator, error) {
	qOp, ok := operators[op]
	if !ok {
		return QueryOperator(0), fmt.Errorf("error parsing operator %s", op)
	}

	return qOp, nil
}
