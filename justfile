default:
  @echo ""
  @just --list

alias c := commit

commit msg="chore: update":
  @just generate
  @git add .
  @git commit -m "{{ msg }}"

alias p := push

push msg="chore: update":
  @just commit "{{ msg }}"
  @git push

alias g := generate

generate:
  @sqlc generate

alias r := run

run:
  @go run ./cmd/worker

alias b := build

build:
  @go build -o bin/worker ./cmd/worker

alias t := test

test:
  @go test ./...

alias ti := test-integration

test-integration:
  @go test -tags=integration ./...
