test:
	go test -race -vet=all ./...

bench:
	go test -bench=. -benchmem

bench-100:
	go run cmd/bench/main.go --workers=100

