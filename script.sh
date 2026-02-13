#!/bin/bash

sudo apt update
sudo apt -y install linux-tools-common linux-tools-generic linux-tools-`uname -r` linux-cloud-tools-`uname -r`

sudo apt -y install llvm llvm-dev
sudo apt -y install clang lld
sudo apt -y install libstdc++-$(clang -v 2>&1 | awk -F/ '/Selected GCC/ {print int($NF)}')-dev

sudo apt -y install cmake ninja-build

sudo apt -y install python3-venv

WAX_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LLVM_PATH=${HOME}/bolt
CLANG_PATH=${HOME}/clang
PROFILE_PATH=${CLANG_PATH}/profiles
OPT_PATH=${CLANG_PATH}/opt
LOG_PATH=${CLANG_PATH}/logs

mkdir -p ${LLVM_PATH}
cd ${LLVM_PATH}
git clone https://github.com/llvm/llvm-project.git

cd ${LLVM_PATH}/llvm-project
git checkout `cat ${WAX_PATH}/bolt/version.txt`
git apply ${WAX_PATH}/bolt/update.patch

mkdir -p ${LLVM_PATH}/build
cd ${LLVM_PATH}/build
cmake -G Ninja ../llvm-project/llvm \
    -DLLVM_TARGETS_TO_BUILD="X86;AArch64" \
    -DCMAKE_BUILD_TYPE=Release \
    -DLLVM_ENABLE_ASSERTIONS=ON \
    -DLLVM_ENABLE_PROJECTS="bolt" \
    -DCMAKE_C_FLAGS="-fno-reorder-blocks-and-partition"
ninja bolt

cd ${WAX_PATH}
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

for VERSION in 14 15; do
    BASE_CLANG_PATH=${CLANG_PATH}/clang-${VERSION}
    LLVM_SOURCES_PATH=${BASE_CLANG_PATH}/sources

    mkdir -p ${LLVM_SOURCES_PATH} && cd ${LLVM_SOURCES_PATH}
    git clone -q --depth=1 --branch=release/${VERSION}.x \
            https://github.com/llvm/llvm-project.git

    CMAKE_FLAGS=(
        "-DLLVM_OPTIMIZED_TABLEGEN=On"
        "-DCMAKE_BUILD_TYPE=RelWithDebInfo"
        "-DLLVM_TARGETS_TO_BUILD=X86"
        "-DLLVM_ENABLE_PROJECTS=clang"
        "-DLLVM_ENABLE_LTO=Full"
        "-DLLVM_USE_LINKER=lld"
        "-DCMAKE_C_COMPILER=/usr/bin/clang"
        "-DCMAKE_CXX_COMPILER=/usr/bin/clang++"
        "-DCMAKE_C_LINK_FLAGS=-Wl,--emit-relocs"
        "-DCMAKE_CXX_LINK_FLAGS=-Wl,--emit-relocs" )

    CLANG_BUILD_PATH=${BASE_CLANG_PATH}/build
    mkdir -p ${CLANG_BUILD_PATH} && cd ${CLANG_BUILD_PATH}
    cmake -G Ninja "${CMAKE_FLAGS[@]}" \
                    ${LLVM_SOURCES_PATH}/llvm-project/llvm
    ninja clang

done

mkdir -p ${PROFILE_PATH}
cd ${PROFILE_PATH}
wget https://raw.githubusercontent.com/facebookarchive/BOLT/old-main/paper/reproduce-bolt-cc2021/inputs.tar.bz2
tar -xjf inputs.tar.bz2 input.cpp

sudo sysctl kernel.perf_event_paranoid=-1
sudo sysctl kernel.dmesg_restrict=0

BIN_PATH=${CLANG_PATH}/clang-14/build/bin/clang-14

perf record -e cycles:u -j any,u -o ${PROFILE_PATH}/clang-14.perf.data \
    -- ${BIN_PATH} ${PROFILE_PATH}/input.cpp -std=c++14 -O2 -c -o ${PROFILE_PATH}/input.o

${LLVM_PATH}/build/bin/perf2bolt \
    -p ${PROFILE_PATH}/clang-14.perf.data \
    -o ${PROFILE_PATH}/clang-14.perf.fdata \
    -w ${PROFILE_PATH}/clang-14.perf.yaml \
    --strict=false \
    ${BIN_PATH}

sudo sysctl kernel.perf_event_paranoid=4
sudo sysctl kernel.dmesg_restrict=1


mkdir -p ${OPT_PATH}
mkdir -p ${LOG_PATH}
for VERSION in 14 15; do
    BIN_PATH=${CLANG_PATH}/clang-${VERSION}/build/bin/clang-${VERSION}
    ${LLVM_PATH}/build/bin/llvm-bolt ${BIN_PATH} \
        -o ${OPT_PATH}/clang-${VERSION}.bolt \
        -data=${PROFILE_PATH}/clang-14.perf.yaml \
        -reorder-blocks=cache+ -reorder-functions=hfsort \
        -split-functions=2 -split-all-cold -split-eh -dyno-stats \
        --print-info &> ${LOG_PATH}/clang-${VERSION}.log
    
    llvm-objdump \
        --line-numbers --no-show-raw-insn --disassemble --section=.text \
        ${BIN_PATH} \
        | gzip -9 > ${LOG_PATH}/clang-${VERSION}.objdump.gz &
done

cd ${WAX_PATH}
source .venv/bin/activate
python3 src/wax.py \
    ${LOG_PATH}/clang-14.log ${LOG_PATH}/clang-15.log \
    ${CLANG_PATH}/clang-14/sources ${CLANG_PATH}/clang-15/sources \
    ${LOG_PATH}/clang-14.objdump.gz ${LOG_PATH}/clang-15.objdump.gz \
    ${PROFILE_PATH}/clang-14.perf.yaml \
    ${LOG_PATH}/clang-15-func_map.csv \
    ${LOG_PATH}/clang-15-bb_map.csv \
    ${LOG_PATH}/clang-15-bb_cross.csv

BIN_PATH=${CLANG_PATH}/clang-15/build/bin/clang-15
${LLVM_PATH}/build/bin/llvm-bolt ${BIN_PATH} \
    -o ${OPT_PATH}/clang-15-wax.bolt \
    -data=${PROFILE_PATH}/clang-14.perf.yaml \
    -reorder-blocks=cache+ -reorder-functions=hfsort \
    -split-functions=2 -split-all-cold -split-eh -dyno-stats \
    --infer-stale-profile \
    --fn-map-file-name=${LOG_PATH}/clang-15-func_map.csv \
    --bb-map-file-name=${LOG_PATH}/clang-15-bb_map.csv \
    --bb-cross-file-name=${LOG_PATH}/clang-15-bb_cross.csv \
    --unique-bb-map

/usr/bin/time -f '%e' ${OPT_PATH}/clang-15.bolt \
    ${PROFILE_PATH}/input.cpp -std=c++14 -O2 -c -o ${PROFILE_PATH}/input.o

/usr/bin/time -f '%e' ${OPT_PATH}/clang-15-wax.bolt \
    ${PROFILE_PATH}/input.cpp -std=c++14 -O2 -c -o ${PROFILE_PATH}/input.o
