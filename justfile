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

alias rc := run-client

run-client:
  @go run ./cmd/client

alias se := start-environment

start-environment:
  @docker compose -f deployments/docker-compose.env.yml up -d

alias b := build

build:
  @go build -o bin/worker ./cmd/worker
  @go build -o bin/client ./cmd/client

alias t := test

test:
  @go test ./...

alias ti := test-integration

test-integration:
  @go test -tags=integration ./...

alias tg := tag

# Bump semver from the latest v* tag and create an annotated release tag.
tag version="patch":
  #!/usr/bin/env bash
  set -euo pipefail

  bump="{{ version }}"
  case "$bump" in
    patch|minor|major) ;;
    *)
      echo "version must be patch, minor, or major (got: $bump)" >&2
      exit 1
      ;;
  esac

  last="$(git tag -l 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -1)"
  if [[ -z "$last" ]]; then
    last="v0.0.0"
  fi

  ver="${last#v}"
  IFS=. read -r major minor patch _ <<< "$ver"

  case "$bump" in
    patch) patch=$((patch + 1)) ;;
    minor) minor=$((minor + 1)); patch=0 ;;
    major) major=$((major + 1)); minor=0; patch=0 ;;
  esac

  new="v${major}.${minor}.${patch}"
  if git rev-parse "$new" >/dev/null 2>&1; then
    echo "tag $new already exists" >&2
    exit 1
  fi

  git tag -a "$new" -m "gowork $new"
  git push origin "$new"
  echo "Created tag $new (from $last)"

