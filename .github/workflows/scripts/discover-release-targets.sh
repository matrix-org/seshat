#!/usr/bin/env bash
# Discovers the native binary assets attached to a GitHub release and works
# out which npm packages need publishing for them.
#
# This is much quicker and more consistent than rebuilding the assets from
# scratch during publish - the binaries were already built by
# build-native.yml and uploaded onto the release by release.yml.
#
# Requires GH_TOKEN, GITHUB_REPOSITORY, RELEASE_TAG and GITHUB_OUTPUT to be
# set in the environment.
#
# Writes a `targets` output: a JSON array of
# {artifact-name, package-name, pkg-os, pkg-cpu}, one per discovered native
# asset, consumed by the publish-native-packages matrix in publish.yml.
set -eu

assets=$(gh release view "$RELEASE_TAG" --repo "$GITHUB_REPOSITORY" \
  --json assets --jq '.assets[].name' | grep '^matrix-seshat-.*\.node$')

entries=()
while IFS= read -r artifact; do
  # For each .node artifact attached to the Github release, determine the
  # package it is intended for...
  stripped="${artifact#matrix-seshat-}"
  stripped="${stripped%.node}"
  IFS='-' read -r os cpu variant <<< "$stripped"
  pkgname="@matrix-org/seshat-$os-$cpu"
  if [ "${variant:-}" = "dynamic" ]; then
    pkgname="${pkgname}-dynamic"
  fi

  # ... and generate a suitably-formatted entry for `targets`
  entry=$(jq -nc --arg artifact "$artifact" --arg package "$pkgname" --arg os "$os" --arg cpu "$cpu" \
    '{"artifact-name": $artifact, "package-name": $package, "pkg-os": $os, "pkg-cpu": $cpu}')
  entries+=("$entry")
done <<< "$assets"

echo "targets=$(printf '%s\n' "${entries[@]}" | jq -cs .)" >> "$GITHUB_OUTPUT"
