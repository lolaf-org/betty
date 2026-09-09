# Security policy

## Reporting a vulnerability

**Do not open a public issue.** Use GitHub's private reporting:
[**Report a vulnerability**](https://github.com/lolaf-org/betty/security/advisories/new). It is visible only to the
maintainer until an advisory is published.

Please include the betty version, the JDK, the JVM flags in use, and whether the session was plain or TLS, along with
enough detail to reproduce it. Betty is maintained by one person: expect an acknowledgement within a week, and a fix
released as soon as one exists rather than on a schedule. You will be credited in the advisory unless you ask not to
be.

## Supported versions

Nothing is released yet. Once 0.1.0 is out, fixes go to the latest released version, and while the version is 0.x
that means the latest minor — there are no maintenance branches.

## Scope

Betty terminates TLS through an `SSLContext` the application supplies, and applies the trust, key and client-auth
settings it is given. A weak cipher suite, an untrusted certificate accepted, or a `RemoteSessionsFilter` that admits
a peer it should not are the application's decisions, not vulnerabilities in betty.

What is in scope is betty doing something the caller did not ask for. Above all, **one session's bytes reaching
another**: read and write buffers are pooled and reused across sessions, so a buffer returned to the pool holding
data that is later handed to a different session's callback is exactly the bug this section exists for. So is a
session becoming usable before the handshake it required completed, a buffer read or written outside its own bounds,
and a peer's input corrupting the state of a session other than its own.
