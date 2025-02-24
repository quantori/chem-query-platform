#!/bin/sh

HOOKS_DIR=".git/hooks"
PRE_COMMIT_HOOK="$HOOKS_DIR/pre-commit"

EXPECTED_HOOK_CONTENT='#!/bin/sh
set -e

echo "🚀 Running pre-commit hook with Spotless..."
./gradlew spotlessApply
git add .

echo "✅ Code formatted with Spotless. Proceeding with commit."
'

mkdir -p "$HOOKS_DIR"

if [ ! -f "$PRE_COMMIT_HOOK" ] || [ "$(cat "$PRE_COMMIT_HOOK")" != "$EXPECTED_HOOK_CONTENT" ]; then
    echo "$EXPECTED_HOOK_CONTENT" > "$PRE_COMMIT_HOOK"
    chmod +x "$PRE_COMMIT_HOOK"
    echo "✅ Pre-commit hook installed!"
else
    echo "✅ Pre-commit hook is already up to date!"
fi
