# Releasing and publishing Seshat

Seshat is released manually using `cargo publish` and `pnpm pack/publish`.

1. Create a release branch

```bash
git switch -c release-4.x.x
```

2. Bump the version in the following files:
   - The Cargo.toml file of the Rust crate
   - The Cargo.toml, Cargo.lock, and package.json file of the Node bindings
   - The CHANGELOG.md file

3. Commit the version bump and create a tag for this commit.
4. After the PR was approved, merge it using a merge commit. Update the tag if
   necessary.
5. Use `cargo publish` in the root folder to publish the Rust crate, you can use
   a `--dry-run` to test things out.
6. Switch to the node bindings folder and use `pnpm pack` to create a package
   and `pnpm publish` to publish the package.
7. Push the tag.
8. Create a release on Github and copy the changelog for the current release.
