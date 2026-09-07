# Releasing and publishing Seshat

The Rust crate is published manually to crates.io. The npm packages
(`@matrix-org/seshat` and its per-platform prebuilt binaries) are published
by CI, triggered by publishing the GitHub Release.

1. Create a release branch

```bash
git switch -c release-5.x.x
```

2. Bump the version in the following files:
   - The Cargo.toml file of the Rust crate
   - The Cargo.toml, Cargo.lock, and package.json file of the Node bindings
   - The CHANGELOG.md file
3. Use `cargo publish --dry-run` in the root folder to test that Cargo is happy.
4. Commit the version bump and create a tag (`x.x.x`) for this commit.
5. Open a PR. After it's approved, merge it using a merge commit. Update the
   tag if necessary.
6. Push the tag. CI builds native binaries for every supported platform/arch
   and creates a **draft** GitHub Release.
7. Review the draft release, then publish it. Publishing it triggers CI to
   publish `@matrix-org/seshat` and its per-platform packages to npm.
8. Use `cargo publish` in the root folder to publish the Rust crate.
