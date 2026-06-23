#!/usr/bin/env node
// Downloads the prebuilt native module for the current platform from the
// matching GitHub release, verifying the SHA256 checksum before writing
// index.node. Falls back to building from source if no prebuilt is available
// or if the download fails.
'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const {execSync} = require('child_process');

const pkg = require('../package.json');

const REPO = 'matrix-org/seshat';
const TAG = pkg.version;
const DEST = path.join(__dirname, '..', 'index.node');

const ARTIFACT_NAME = `matrix-seshat-${process.platform}-${process.arch}.node`;

/**
 * Fetches a buffer from a URL.
 * @param {string} url The URL to fetch from.
 * @return {Buffer} The buffer fetched.
 */
async function fetchBuffer(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
    return Buffer.from(await res.arrayBuffer());
}

/**
 * Fall back and build the module from source.
 */
function buildFromSource() {
    console.log('Building matrix-seshat from source (bundled-sqlcipher)...');
    const dir = path.join(__dirname, '..');
    const execpath = process.env.npm_execpath;
    const cmd = execpath ?
        (execpath.endsWith('.js') ? `node "${execpath}"` : `"${execpath}"`) +
          ' run build-bundled' :
        'yarn run build-bundled';
    execSync(cmd, {cwd: dir, stdio: 'inherit'});
}

/**
 * Main function.
 */
async function main() {
    if (fs.existsSync(DEST)) {
        console.log(`${DEST} already exists, skipping download.`);
        return;
    }

    // Fetch release asset list from the GitHub API.
    let assets;
    try {
        console.log(`Fetching release ${TAG} from ${REPO}...`);
        const res = await fetch(
            `https://api.github.com/repos/${REPO}/releases/tags/${TAG}`,
            {
                headers: {
                    Accept: 'application/vnd.github.v3+json',
                },
            },
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        assets = (await res.json()).assets;
    } catch (e) {
        console.warn(`Could not fetch release metadata: ${e.message}`);
        buildFromSource();
        return;
    }

    const nodeAsset = assets.find((a) => a.name === ARTIFACT_NAME);
    if (!nodeAsset) {
        console.log(
            `Release ${TAG} has no ${ARTIFACT_NAME}, building from source.`,
        );
        buildFromSource();
        return;
    }

    // Download the checksums file and find the expected hash for our artifact.
    let expectedSha;
    const checksumsAsset = assets.find((a) => a.name === 'checksums.txt');
    if (checksumsAsset) {
        try {
            const buf = await fetchBuffer(checksumsAsset.browser_download_url);
            const line = buf
                .toString()
                .split('\n')
                .find((l) => l.includes(ARTIFACT_NAME));
            if (line) {
                expectedSha = line.trim().split(/\s+/)[0];
            } else {
                console.warn(`No entry for ${ARTIFACT_NAME} in checksums.txt.`);
            }
        } catch (e) {
            console.warn(`Could not fetch checksums: ${e.message}`);
        }
    } else {
        console.warn(
            'No checksums.txt in this release, skipping verification.',
        );
    }

    // Download the native module.
    console.log(`Downloading ${ARTIFACT_NAME}...`);
    let data;
    try {
        data = await fetchBuffer(nodeAsset.browser_download_url);
    } catch (e) {
        console.warn(`Download failed: ${e.message}`);
        buildFromSource();
        return;
    }

    // Verify the checksum before writing anything to disk.
    if (expectedSha) {
        const actual = crypto.createHash('sha256').update(data).digest('hex');
        if (actual !== expectedSha) {
            throw new Error(
                `SHA256 mismatch for ${ARTIFACT_NAME}:
expected ${expectedSha}
got      ${actual}`,
            );
        }
        console.log('Checksum verified.');
    }

    fs.writeFileSync(DEST, data);
    console.log(`Wrote ${DEST}`);
}

main().catch((e) => {
    console.error(e.message);
    process.exit(1);
});
