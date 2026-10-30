# Agents

## The Java original

This ports [java-envoy-xds](https://github.com/JonathanO/java-envoy-xds) to Go, and should behave the same. Its tests are
ported from the Java ones, as near as makes sense: the same scenarios and assertions, in idiomatic Go. To add or change
a test, start from the Java test for that behaviour. A ported test that fails shows a difference between the two:
report it, rather than quietly changing either side. When you change behaviour or a test here, make the same change
there, or say why it doesn't apply.

## Code

Modern, idiomatic Go. Use the standard library over a dependency wherever it does the job.

## Checks

- `gofmt -l .` prints nothing, and `go vet ./...` and `go test -race ./...` pass.
- `gomutants ./...` must kill every mutant, each package with its own tests. CI tests the mutants on a pull request's
  changed lines, and all of them on main. An exception needs a strong reason, agreed with the maintainer.
- The e2e module runs the conformance suite against real Envoys: `cd e2e && go test ./...`, which needs Docker. Never
  mock Envoy.
- The spectest module runs the trace vectors from [envoy-xds-spec](https://github.com/JonathanO/envoy-xds-spec), the
  submodule at `spec/`: `git submodule update --init`, then `cd spectest && go test ./...`. A behaviour change needs a
  vector there, which fails without it.
