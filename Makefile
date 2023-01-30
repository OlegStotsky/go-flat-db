test:
	go test -race -vet=all ./...

bench:
	go test -bench=. -benchmem
