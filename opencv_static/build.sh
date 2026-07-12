#!/bin/bash

cmake -S . -B build \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release

cmake --build build --parallel

cmake --install build --prefix "$PWD/build/opencv-install"
ln -s build/opencv-install/lib/opencv4/3rdparty/liblibjpeg-turbo.a build/opencv-install/lib
ln -s build/opencv-install/lib/opencv4/3rdparty/liblibjpeg-turbo.a build/opencv-install/lib/libjpeg-turbo.a