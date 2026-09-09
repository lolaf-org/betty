# Contributing to betty

Issues and pull requests are welcome. Betty is a small library with one maintainer, so the most useful thing a change
can be is *narrow*: one topic, tests that fail without it, and a description of what you measured.

## Building

```bash
./mvnw -T1C clean install
```

**JDK 17 or newer is required to build**, even though the library itself runs on JDK 11 and ships Java 11 bytecode:
the test stack needs 17, and the shared surefire arguments include a flag that JDK 16 and older reject outright. The
build enforces this and says so.

**No toolchains are needed.** Nothing in betty reaches a `java.base` internal at compile time, so every module
compiles with `maven.compiler.release=11` under whichever JDK is running Maven, and CI provisions one.

## What the build checks, beyond the tests

Several things fail the build that are easy to trip over and easy to fix:

- **Javadoc references.** Javadoc runs with `-Xdoclint:all,-missing`, so a `{@link}`, `@see` or `@throws` that does
  not resolve is an error, as is raw HTML in a comment — write `{@code List<String>}`, not `List<String>`. It reads a
  delomboked copy of the sources, which is why a `{@link}` to a Lombok-generated accessor resolves.
- **Licence headers.** Every source file carries the Apache header from `HEADER.txt`; `./mvnw license:format` adds it
  to a new file.
- **The module path.** `betty-module-path-test` resolves the built jars as JPMS modules, so two jars may never
  contribute the same package, and every jar needs its `automatic.module.name` property. A new module that forgets it
  fails that test rather than shipping a name derived from its filename.
- **Enforcer rules**, which state the Maven and JDK minimums above, and forbid a release depending on a snapshot.

## Tests

- **Do not give a module its own `<argLine>`.** It replaces the shared surefire arguments wholesale, and what falls
  out is silent. Add arguments with `surefire.argLine.extra` instead.
- **The tests open real sockets on localhost.** Test classes run in parallel and each test instance binds a free port
  of its own, so classes cannot collide — a new test must take its port the same way rather than hard-coding one.
- A few tests assert throughput behind a timeout. On a loaded machine those are the first to go amber; re-run the
  test alone before concluding a change broke it.

## Changes to the IO path

The read and write callbacks run on the IO thread that owns the session, and that is why this library exists. On
those paths:

- An optional behaviour must cost nothing when it is off. The idiom is a strategy selected once — an interface with
  an active implementation and a do-nothing one, as `IOStats` and `VoidStats` are — not a branch per event.
- Nothing allocates on the happy path: no strings, no exceptions, no collections, no boxing, no capturing lambda.
  The buffer handed to `onRead` comes from a pool and goes back to it when the callback returns.
- If a change there is not obviously free, measure it. `benchmarks/` is a JMH suite comparing betty against Netty,
  Aeron, Mina, Jetty and two hand-written NIO clients:

  ```bash
  ./mvnw clean install -pl benchmarks -am
  cd benchmarks && ./run-benchmark.sh
  ```

  Put the before and after numbers in the pull request. Read `benchmarks/README.md` first — results are
  machine-specific, and a client that does not support a parameter combination fails the run by design.

## Pull requests

Work on a branch, open a PR against `main`, and let CI finish — it builds and tests every module on a cold runner.
There is no CLA: contributions are under the [Apache License 2.0](LICENSE.txt), the same licence the project ships
under.

Security problems do not go in an issue or a PR — see [SECURITY.md](SECURITY.md).
