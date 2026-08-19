#!/bin/sh
set -eu

abi=${1:?usage: build-android-library.sh ABI [OUTPUT_DIR]}
output_dir=${2:-dist/android_$abi}
ndk_home=${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-}}
api_level=${ANDROID_API_LEVEL:-24}

if [ -z "$ndk_home" ]; then
    echo "ANDROID_NDK_HOME or ANDROID_NDK_ROOT is required" >&2
    exit 1
fi

case "$abi" in
    arm64-v8a)
        goarch=arm64
        compiler=aarch64-linux-android
        goarm=
        ;;
    armeabi-v7a)
        goarch=arm
        compiler=armv7a-linux-androideabi
        goarm=7
        ;;
    x86_64)
        goarch=amd64
        compiler=x86_64-linux-android
        goarm=
        ;;
    x86)
        goarch=386
        compiler=i686-linux-android
        goarm=
        ;;
    *)
        echo "Unsupported Android ABI: $abi" >&2
        exit 1
        ;;
esac

host_tag=linux-x86_64
cc="$ndk_home/toolchains/llvm/prebuilt/$host_tag/bin/${compiler}${api_level}-clang"
if [ ! -x "$cc" ]; then
    echo "Android compiler not found: $cc" >&2
    exit 1
fi

mkdir -p "$output_dir"
GOOS=android \
GOARCH="$goarch" \
GOARM="$goarm" \
CGO_ENABLED=1 \
CC="$cc" \
go build -ldflags='-checklinkname=0' -buildmode=c-shared \
    -o "$output_dir/libhuginn_messenger.so" .
