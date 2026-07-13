cmake_minimum_required(VERSION 3.24)

project(build_opencv LANGUAGES C CXX)

include(FetchContent)

# ------------------------------------------------------------
# 全局构建配置
# ------------------------------------------------------------

# 构建静态库，而不是 .so
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

# Rust 最终如果生成 cdylib/shared library，需要 OpenCV 带 PIC。
# 对普通 Rust 可执行文件也没有坏处。
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ------------------------------------------------------------
# 只编译需要的 OpenCV 模块
# ------------------------------------------------------------

set(BUILD_LIST "core,imgproc,imgcodecs" CACHE STRING "" FORCE)

# ------------------------------------------------------------
# 关闭不需要的目标
# ------------------------------------------------------------

set(BUILD_opencv_apps OFF CACHE BOOL "" FORCE)
set(BUILD_opencv_python_bindings_generator OFF CACHE BOOL "" FORCE)
set(BUILD_opencv_python2 OFF CACHE BOOL "" FORCE)
set(BUILD_opencv_python3 OFF CACHE BOOL "" FORCE)
set(BUILD_opencv_java_bindings_generator OFF CACHE BOOL "" FORCE)
set(BUILD_JAVA OFF CACHE BOOL "" FORCE)

set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_PERF_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_DOCS OFF CACHE BOOL "" FORCE)

# ------------------------------------------------------------
# 关闭不需要的后端
# ------------------------------------------------------------

set(WITH_FFMPEG OFF CACHE BOOL "" FORCE)
set(WITH_GSTREAMER OFF CACHE BOOL "" FORCE)
set(WITH_GTK OFF CACHE BOOL "" FORCE)
set(WITH_QT OFF CACHE BOOL "" FORCE)

set(WITH_OPENCL OFF CACHE BOOL "" FORCE)
set(WITH_IPP OFF CACHE BOOL "" FORCE)
set(WITH_ITT OFF CACHE BOOL "" FORCE)
set(WITH_TBB OFF CACHE BOOL "" FORCE)
set(WITH_OPENMP OFF CACHE BOOL "" FORCE)

# 硬件/平台相关功能
set(WITH_V4L OFF CACHE BOOL "" FORCE)
set(WITH_1394 OFF CACHE BOOL "" FORCE)
set(WITH_VA OFF CACHE BOOL "" FORCE)
set(WITH_VA_INTEL OFF CACHE BOOL "" FORCE)
set(WITH_VULKAN OFF CACHE BOOL "" FORCE)

# ------------------------------------------------------------
# 图像格式
# ------------------------------------------------------------

# 只保留 JPEG
set(WITH_JPEG ON CACHE BOOL "" FORCE)
set(BUILD_JPEG ON CACHE BOOL "" FORCE)

set(WITH_AVIF OFF CACHE BOOL "" FORCE)
set(WITH_PNG OFF CACHE BOOL "" FORCE)
set(WITH_WEBP OFF CACHE BOOL "" FORCE)
set(WITH_TIFF OFF CACHE BOOL "" FORCE)
set(WITH_OPENEXR OFF CACHE BOOL "" FORCE)
set(WITH_JASPER OFF CACHE BOOL "" FORCE)
set(WITH_OPENJPEG OFF CACHE BOOL "" FORCE)
set(WITH_GDAL OFF CACHE BOOL "" FORCE)
set(WITH_GDCM OFF CACHE BOOL "" FORCE)

# 避免下载不必要的数据
set(OPENCV_DOWNLOAD_PATH
    "${CMAKE_BINARY_DIR}/opencv-downloads"
    CACHE PATH ""
    FORCE
)

# ------------------------------------------------------------
# 获取 OpenCV
# ------------------------------------------------------------

FetchContent_Declare(
    opencv
    GIT_REPOSITORY https://github.com/opencv/opencv.git
    GIT_TAG        4.12.0
    GIT_SHALLOW    TRUE
    GIT_PROGRESS   TRUE
)

FetchContent_MakeAvailable(opencv)
