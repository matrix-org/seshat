# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- [[#67]] Added support to store user specific versions in the database.
- [[#65]] Added a method to check if a particular room is already indexed.
- [[#64]] Exposed a method to change the passphrase in the js bindings.
- [[#63]] Added support to paginate search results.
- [[#60]] Added serde serialize/deserialize implementations for most of our
      structs.

### Changed

- [[#66]] Changed the return type of the search methods, they now return a
      struct instead of a tuple. **This is a breaking change**.
- [[#62]] Made all encryption specific dependencies optional.
- [[#59]] Switched from failure to thiserror for our error types.

[#67]: https://github.com/matrix-org/seshat/pull/67
[#66]: https://github.com/matrix-org/seshat/pull/66
[#65]: https://github.com/matrix-org/seshat/pull/65
[#64]: https://github.com/matrix-org/seshat/pull/64
[#63]: https://github.com/matrix-org/seshat/pull/63
[#62]: https://github.com/matrix-org/seshat/pull/62
[#60]: https://github.com/matrix-org/seshat/pull/60
[#59]: https://github.com/matrix-org/seshat/pull/59
