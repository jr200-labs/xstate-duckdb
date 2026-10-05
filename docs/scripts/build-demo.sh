#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
# GAT's Quarto renderer invokes this hook from a clean checkout.
# Use the exact package manager declared by the repository on CI and locally.
manager=$(node -p 'require("./package.json").packageManager')
npx --yes "$manager" install --frozen-lockfile
npx --yes "$manager" run demo:build
