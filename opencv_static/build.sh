#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
build_dir="${OPENCV_BUILD_DIR:-$script_dir/build}"
install_prefix="${OPENCV_INSTALL_PREFIX:-$build_dir/opencv-install}"
cc="${CC:-clang}"
cxx="${CXX:-clang++}"

if [[ -f "$build_dir/CMakeCache.txt" ]]; then
    cached_cc="$(sed -n 's/^CMAKE_C_COMPILER:[^=]*=//p' "$build_dir/CMakeCache.txt")"
    cached_cxx="$(sed -n 's/^CMAKE_CXX_COMPILER:[^=]*=//p' "$build_dir/CMakeCache.txt")"
    cached_cc="$(readlink -f "$cached_cc")"
    cached_cxx="$(readlink -f "$cached_cxx")"
    requested_cc="$(readlink -f "$(command -v "$cc")")"
    requested_cxx="$(readlink -f "$(command -v "$cxx")")"
    if [[ "$cached_cc" != "$requested_cc" || "$cached_cxx" != "$requested_cxx" ]]; then
        if [[ -z "$build_dir" || "$build_dir" == / || "$build_dir" == "$script_dir" ]]; then
            echo "refusing to clean unsafe OpenCV build directory: $build_dir" >&2
            exit 1
        fi
        rm -rf -- "$build_dir"
    fi
fi

cmake -S "$script_dir" -B "$build_dir" \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER="$cc" \
    -DCMAKE_CXX_COMPILER="$cxx" \
    -DCMAKE_INSTALL_PREFIX="$install_prefix"

cmake --build "$build_dir" --parallel
cmake --install "$build_dir"

jpeg_archive="$install_prefix/lib/opencv4/3rdparty/liblibjpeg-turbo.a"
test -f "$jpeg_archive"
ln -sfn "opencv4/3rdparty/liblibjpeg-turbo.a" \
    "$install_prefix/lib/liblibjpeg-turbo.a"
ln -sfn "opencv4/3rdparty/liblibjpeg-turbo.a" \
    "$install_prefix/lib/libjpeg-turbo.a"
