#!/bin/sh
# Go 1.27.4+ refuses to link cgo archives that carry LLVM bitcode sections.
# zerobus-sdk's prebuilt archives ship rustc's embedded bitcode (.llvmbc/.llvmcmd
# on ELF, __LLVM,__bitcode/__cmdline on Mach-O), so strip those sections in the
# module cache before building. Bitcode is only used for LTO; removing it doesn't
# change the linked code. Archives whose tool isn't installed are skipped.
# Remove once zerobus-sdk publishes archives built without embedded bitcode.
set -eu

dir=$(go mod download -json github.com/databricks/zerobus-sdk/go | sed -n 's/^[[:space:]]*"Dir": "\(.*\)",$/\1/p')
if [ -z "$dir" ]; then
	echo "zerobus-sdk module directory not found" >&2
	exit 1
fi
chmod -R u+w "$dir/lib"

for pair in \
	linux_amd64:x86_64-linux-gnu-objcopy \
	linux_arm64:aarch64-linux-gnu-objcopy \
	darwin_amd64:llvm-objcopy \
	darwin_arm64:llvm-objcopy; do
	arch=${pair%%:*}
	tool=${pair#*:}
	if ! command -v "$tool" >/dev/null; then
		echo "skipping $arch: $tool not found"
		continue
	fi
	"$tool" \
		--remove-section=.llvmbc --remove-section=.llvmcmd \
		--remove-section=__LLVM,__bitcode --remove-section=__LLVM,__cmdline \
		"$dir/lib/$arch/libzerobus_ffi.a"
	echo "stripped bitcode from $arch"
done
