#!/usr/bin/env node
// Downloads the prebuilt native module for a given platform/arch from the
// matching GitHub release, verifying the SHA256 checksum before writing
// index.node. Falls back to building from source if no prebuilt is available
// or if the download fails, unless disabled via --no-fallback.
//
// Runs as the package's `postinstall` for normal npm/yarn installs (target
// defaults to the host running the install), but can also be invoked
// directly with an explicit --platform/--arch/--dest, e.g. by a downstream
// build system cross-compiling for a different target than the host.

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import {execSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';

const require = createRequire(import.meta.url);
const pkg = require('../package.json');

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const REPO = 'matrix-org/seshat';
const TAG: string = pkg.version;

type SqlcipherVariant = 'static' | 'dynamic';

interface CliOptions {
    fallback: boolean;
    variant: SqlcipherVariant;
    platform?: string;
    arch?: string;
    dest?: string;
    token?: string;
}

interface DownloadOptions {
    /**
     * Target platform (e.g. process.platform)
     */
    platform: string;
    /**
     * Target arch (e.g. process.arch).
     */
    arch: string;
    /**
     *  Path to write the downloaded module to.
     */
    dest: string;
    /**
     * Whether to build from source on failure.
     */
    fallback: boolean;
    /**
     * Either "static" (bundled sqlcipher, the default) or "dynamic" (link against the system sqlcipher).
     */
    variant: SqlcipherVariant;
    /**
     * Optional GitHub bearer token to fetch the binary with.
     */
    token?: string;
}

interface GithubAsset {
    name: string;
    browser_download_url: string;
}

/**
 * Parses --key=value / --flag style arguments.
 * @param argv Arguments to parse, excluding node/script path.
 * @return Parsed options.
 */
function parseArgs(argv: string[]): CliOptions {
    const opts: CliOptions = {fallback: true, variant: 'static'};
    for (const arg of argv) {
        if (arg === '--no-fallback') {
            opts.fallback = false;
        } else if (arg.startsWith('--platform=')) {
            opts.platform = arg.slice('--platform='.length);
        } else if (arg.startsWith('--arch=')) {
            opts.arch = arg.slice('--arch='.length);
        } else if (arg.startsWith('--dest=')) {
            opts.dest = arg.slice('--dest='.length);
        } else if (arg.startsWith('--sqlcipher=')) {
            opts.variant = arg.slice('--sqlcipher='.length) as SqlcipherVariant;
        } else if (arg.startsWith('--token=')) {
            opts.token = arg.slice('--token='.length);
        }
    }
    if (opts.variant !== 'static' && opts.variant !== 'dynamic') {
        throw new Error(
            `--sqlcipher must be "static" or "dynamic", got "${opts.variant}"`,
        );
    }
    return opts;
}

/**
 * Fetches a buffer from a URL.
 * @param url The URL to fetch from.
 * @return The buffer fetched.
 */
async function fetchBuffer(url: string): Promise<Buffer> {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
    return Buffer.from(await res.arrayBuffer());
}

/**
 * Builds the module from source in the given directory.
 * @param dir The package directory to build in.
 * @param variant Either "static" (bundled sqlcipher) or "dynamic"
 *   (link against the system sqlcipher).
 */
function buildFromSource(dir: string, variant: SqlcipherVariant): void {
    const script = variant === 'dynamic' ? 'build' : 'build-bundled';
    console.log(
        `Building matrix-seshat from source (${variant} sqlcipher)...`,
    );
    const execpath = process.env.npm_execpath;
    const cmd = execpath ?
        (execpath.endsWith('.js') ? `node "${execpath}"` : `"${execpath}"`) +
          ` run ${script}` :
        `yarn run ${script}`;
    execSync(cmd, {cwd: dir, stdio: 'inherit'});
}

/**
 * Downloads and verifies the prebuilt native module for a target
 * platform/arch, writing it to `dest`. Optionally falls back to building
 * from source in the destination's directory on any failure.
 * @return Whether index.node now exists at `dest`.
 */
async function downloadPrebuilt(
    {platform, arch, dest, fallback, variant, token}: DownloadOptions,
): Promise<boolean> {
    const dir = path.dirname(dest);
    const suffix = variant === 'dynamic' ? '-dynamic' : '';
    const artifactName = `matrix-seshat-${platform}-${arch}${suffix}.node`;

    if (fs.existsSync(dest)) {
        console.log(`${dest} already exists, skipping download.`);
        return true;
    }

    // Fetch release asset list from the GitHub API.
    let assets: GithubAsset[];
    try {
        console.log(`Fetching release ${TAG} from ${REPO}...`);
        const res = await fetch(
            `https://api.github.com/repos/${REPO}/releases/tags/${TAG}`,
            {
                headers: {
                    Accept: 'application/vnd.github.v3+json',
                    ...(token ? {Authorization: `Bearer ${token}`} : {}),
                },
            },
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        assets = (await res.json()).assets;
    } catch (e) {
        console.warn(`Could not fetch release metadata: ${(e as Error).message}`);
        if (!fallback) return false;
        buildFromSource(dir, variant);
        return true;
    }

    const nodeAsset = assets.find((a) => a.name === artifactName);
    if (!nodeAsset) {
        console.log(
            `Release ${TAG} has no ${artifactName}, ` +
              (fallback ? 'building from source.' : 'giving up.'),
        );
        if (!fallback) return false;
        buildFromSource(dir, variant);
        return true;
    }

    // Download the checksums file and find the expected hash for our artifact.
    let expectedSha: string | undefined;
    const checksumsAsset = assets.find((a) => a.name === 'checksums.txt');
    if (checksumsAsset) {
        try {
            const buf = await fetchBuffer(checksumsAsset.browser_download_url);
            const line = buf
                .toString()
                .split('\n')
                .find((l) => l.includes(artifactName));
            if (line) {
                expectedSha = line.trim().split(/\s+/)[0];
            } else {
                console.warn(`No entry for ${artifactName} in checksums.txt.`);
            }
        } catch (e) {
            console.warn(`Could not fetch checksums: ${(e as Error).message}`);
        }
    } else {
        console.warn(
            'No checksums.txt in this release, skipping verification.',
        );
    }

    // Download the native module.
    console.log(`Downloading ${artifactName}...`);
    let data: Buffer;
    try {
        data = await fetchBuffer(nodeAsset.browser_download_url);
    } catch (e) {
        console.warn(`Download failed: ${(e as Error).message}`);
        if (!fallback) return false;
        buildFromSource(dir, variant);
        return true;
    }

    // Verify the checksum before writing anything to disk.
    if (expectedSha) {
        const actual = crypto.createHash('sha256').update(data).digest('hex');
        if (actual !== expectedSha) {
            throw new Error(
                `SHA256 mismatch for ${artifactName}:
expected ${expectedSha}
got      ${actual}`,
            );
        }
        console.log('Checksum verified.');
    }

    fs.writeFileSync(dest, data);
    console.log(`Wrote ${dest}`);
    return true;
}

/**
 * CLI entry point.
 */
async function main(): Promise<void> {
    const opts = parseArgs(process.argv.slice(2));
    const platform = opts.platform || process.platform;
    const arch = opts.arch || process.arch;
    const dest = opts.dest || path.join(__dirname, '..', 'index.node');

    const ok = await downloadPrebuilt({
        platform,
        arch,
        dest,
        fallback: opts.fallback,
        variant: opts.variant,
        token: opts.token,
    });

    if (!ok) process.exit(1);
}

if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch((e) => {
        console.error((e as Error).message);
        process.exit(1);
    });
}

export {downloadPrebuilt};
