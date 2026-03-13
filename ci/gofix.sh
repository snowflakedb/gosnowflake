#!/usr/bin/env bash
set -euo pipefail

CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$CI_DIR/.."

GOOS_LIST=(linux darwin windows)
GOARCH_LIST=(amd64 arm64)

# Standard GOOS/GOARCH values — handled by the matrix, not via -tags.
# Version tags (go1.X) and toolchain tags (gc, gccgo, ignore) are also excluded.
STANDARD_TAGS=(
  linux darwin windows freebsd openbsd netbsd plan9 solaris aix js wasip1 android ios
  amd64 arm64 386 arm mips mips64 mipsle mips64le ppc64 ppc64le riscv64 s390x wasm
  gc gccgo ignore
)

# Automatically discover custom build tags from //go:build lines.
# Strips boolean operators and negations, deduplicates, then removes
# standard tags and go1.X version constraints.
discover_custom_tags() {
  grep -rh '//go:build' --include='*.go' . \
    | sed 's|//go:build||g' \
    | tr '!&|() \t' '\n' \
    | grep -v '^$' \
    | sort -u \
    | while IFS= read -r tag; do
        # Skip go1.X version tags
        [[ "$tag" =~ ^go[0-9] ]] && continue
        # Skip standard GOOS/GOARCH/toolchain tags
        local skip=false
        for std in "${STANDARD_TAGS[@]}"; do
          [[ "$tag" == "$std" ]] && skip=true && break
        done
        $skip || echo "$tag"
      done
}

mapfile -t CUSTOM_TAGS < <(discover_custom_tags)
TAGS_LIST=("" "${CUSTOM_TAGS[@]}")

TOTAL=$(( ${#GOOS_LIST[@]} * ${#GOARCH_LIST[@]} * ${#TAGS_LIST[@]} ))
RUN=0

echo "Discovered custom build tags: ${CUSTOM_TAGS[*]:-none}"
echo "Running go fix across all OS/arch/tag combinations..."

for os in "${GOOS_LIST[@]}"; do
  for arch in "${GOARCH_LIST[@]}"; do
    for tags in "${TAGS_LIST[@]}"; do
      RUN=$(( RUN + 1 ))
      tag_flag=""
      tag_label="(no tags)"
      if [[ -n "$tags" ]]; then
        tag_flag="-tags=$tags"
        tag_label="tags=$tags"
      fi
      echo "  [$RUN/$TOTAL] GOOS=$os GOARCH=$arch $tag_label"
      # "no cgo types" is a harmless warning from go/packages when it cannot
      # invoke the cgo preprocessor (cross-compilation, no C toolchain, etc.).
      # No go fix fixer depends on cgo type information, so suppress the noise.
      CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go fix $tag_flag ./... \
        2> >(grep -v "^go fix: warning: no cgo types:" >&2)
    done
  done
done

echo "Checking for uncommitted changes..."
if ! git diff --exit-code; then
  echo ""
  echo "ERROR: go fix produced changes."
  echo "Run 'ci/gofix.sh' locally and commit the result."
  exit 1
fi

echo "All files are up to date."
