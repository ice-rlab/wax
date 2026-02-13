# <span style="font-variant:small-caps;">Wax</span>: Optimizing Data Center Applications With Stale Profile

<span style="font-variant:small-caps;">Wax</span>  is a novel technique for optimizing data center applications with stale profiles. Data center providers, [Google](https://dl.acm.org/doi/abs/10.1145/2854038.2854044) and [Meta](https://dl.acm.org/doi/abs/10.1145/3640537.3641573), continuously profile the current versions of data center applications and use these profiles to optimize subsequent versions. As practitioners deploy new versions every one or two weeks, source code changes rapidly between them, causing 70-92% of profile samples to become stale. <span style="font-variant:small-caps;">Wax</span> addresses this profile staleness with the key insight of using the debug and source code information. Leveraging source and debug information, <span style="font-variant:small-caps;">Wax</span> provides 6-26% performance speedups across five widely used data center applications, achieving 65-93% of the benefits of fresh profiles.

Please cite the following [paper](https://takhandipu.github.io/papers/bhuiyan-wax-asplos-2026.pdf) if you use <span style="font-variant:small-caps;">Wax</span>'s open-source artifacts:
```
@inproceedings{bhuiyan-wax-asplos-2026,
  title={Wax: Optimizing Data Center Applications With Stale Profile},
  author={Bhuiyan, Tawhid and Hoque, Sumya and Moreira, Angélica and Khan, Tanvir Ahmed},
  booktitle={Proceedings of the 31st ACM International Conference on Architectural Support for Programming Languages and Operating Systems (**ASPLOS**), Volume 2},
  year={2026},
  location = {Pittsburgh, PA, USA},
  series = {ASPLOS '26},
  publisher = {Association for Computing Machinery (ACM)}
}
```

## Running the Experiments with Script
To run all experiments using a single command, clone this repository and execute [script.sh](script.sh). If you prefer to run the experiments step by step, follow the instructions in the sections below.

## Pre-requisite

### Requirements:
- Intel CPU with `LBR` support
- Free storage: 26GB

### Tested setup:
- Operating system: Ubuntu 24.04
- CPU: Intel Xeon Gold 5512U (28 cores)
- RAM: 128GB
- Python: 3.12.3
- Total time: 1.5 hours

### Tools:
- Update the system
    ```
    sudo apt update
    ```
- Install `perf` with the following commands:
    ```
    sudo apt -y install linux-tools-common linux-tools-generic linux-tools-`uname -r` linux-cloud-tools-`uname -r`
    ```
    The CPU needs to support `LBR`. To check whether it supports `LBR`, run 
    ```
    perf record -e cycles:u -j any,u -- echo "test"
    ```
    It will create a `perf.data` file if `LBR` is supported. Otherwise, it will show the error: `cycles: PMU Hardware doesn't support sampling/overflow-interrupts. Try 'perf stat'`.

    *Note: It may require permissions to run `perf` command, which can be enabled with*
    ```
    sudo sysctl kernel.perf_event_paranoid=-1
    sudo sysctl kernel.dmesg_restrict=0
    ```
- Install `llvm` tools with the following commands:
    ```
    sudo apt -y install llvm llvm-dev
    sudo apt -y install clang lld
    sudo apt -y install libstdc++-$(clang -v 2>&1 | awk -F/ '/Selected GCC/ {print int($NF)}')-dev
    ```
- Install `CMake` and `Ninja` with the following command:
    ```
    sudo apt -y install cmake ninja-build
    ```
- Install Python `venv` with the following command:
    ```
    sudo apt -y install python3-venv
    ```

## Getting Started

### Initializing variables
To keep track of paths to different directories, we initialize some variables. Make changes as you see fit.
```
WAX_PATH=${HOME}/wax
LLVM_PATH=${HOME}/bolt
CLANG_PATH=${HOME}/clang
PROFILE_PATH=${CLANG_PATH}/profiles
LOG_PATH=${CLANG_PATH}/logs
OPT_PATH=${CLANG_PATH}/opt
```
Here, 
- `WAX_PATH` is the directory where this repository is cloned
- `LLVM_PATH` points to our patched version of LLVM
- `CLANG_PATH` is the directory to the clang used for benchmarking
- `PROFILE_PATH` is the place where profiles are stored
- `OPT_PATH` is the directory where the optimized clang binary is stored
- `LOG_PATH` is the directory where intermediate logs (used by <span style="font-variant:small-caps;">Wax</span>) are stored

### Install `llvm-bolt` from source with <span style="font-variant:small-caps;">Wax</span>'s patch

Clone LLVM from GitHub with the following commands: (*Please note that cloning LLVM may require a modest amount of time. For example, it took us around 2.5 minutes to clone LLVM on [our setup](#tested-setup).*)
```
mkdir -p ${LLVM_PATH}
cd ${LLVM_PATH}
git clone https://github.com/llvm/llvm-project.git
```

To apply our patch on LLVM and build it for <span style="font-variant:small-caps;">Wax</span>, run the following: (*Again, building llvm-bolt from source may require a modest amount of time. It took us around 3.5 minutes to complete the following script on [our setup](#tested-setup).*)
```
git clone https://github.com/ice-rlab/wax.git ${WAX_PATH}
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
```

### Setup Python libraries
Run the following scripts to install the required Python libraries in a Python virtual environment
```
cd ${WAX_PATH}
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Applying <span style="font-variant:small-caps;">Wax</span> to `clang`

- Stale: `clang-14`
- Fresh: `clang-15`

### Install `clang-14` and `clang-15` from source

To install clang that allows applying BOLT and <span style="font-variant:small-caps;">Wax</span>, run the following: (*Building the two clang versions may take a substantial amount of time. For example, it took us more than an hour to run it on [our setup](#tested-setup).*)
```
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
```

### Setup workload
As a workload, we use a large template-heavy LLVM source file (similar to [prior work](https://dl.acm.org/doi/abs/10.1145/3446804.3446843))
```
mkdir -p ${PROFILE_PATH}
cd ${PROFILE_PATH}
wget https://raw.githubusercontent.com/facebookarchive/BOLT/old-main/paper/reproduce-bolt-cc2021/inputs.tar.bz2
tar -xjf inputs.tar.bz2 input.cpp
```

### Collect profile from `clang-14`
If required, first setup permissions required to run `perf` command.
```
sudo sysctl kernel.perf_event_paranoid=-1
sudo sysctl kernel.dmesg_restrict=0
```
Record LBR profile
```
BIN_PATH=${CLANG_PATH}/clang-14/build/bin/clang-14

perf record -e cycles:u -j any,u -o ${PROFILE_PATH}/clang-14.perf.data \
    -- ${BIN_PATH} ${PROFILE_PATH}/input.cpp -std=c++14 -O2 -c -o ${PROFILE_PATH}/input.o
```
Process `perf.data` so that BOLT can use it for PGO
```
${LLVM_PATH}/build/bin/perf2bolt \
    -p ${PROFILE_PATH}/clang-14.perf.data \
    -o ${PROFILE_PATH}/clang-14.perf.fdata \
    -w ${PROFILE_PATH}/clang-14.perf.yaml \
    --strict=false \
    ${BIN_PATH}
```

(Optional) In the end, reset the permissions as follows
```
sudo sysctl kernel.perf_event_paranoid=4
sudo sysctl kernel.dmesg_restrict=1
```

### Gather the required information for <span style="font-variant:small-caps;">Wax</span>
To read basic-block and control-flow graph using BOLT, and to read debug information using `objdump`, run the following:
(*Please note that running the following scripts may require a modest amount of time. For example, it took us around 4.5 minutes to run it on [our setup](#tested-setup).*)
```
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
```
In the above codes, `${LOG_PATH}/clang-${VERSION}.log` contains basic-block and control-flow graph information. `${LOG_PATH}/clang-${VERSION}.objdump.gz` contains debug information along with the assembly instructions.

### Map stale to fresh profile using <span style="font-variant:small-caps;">Wax</span>
Run <span style="font-variant:small-caps;">Wax</span>'s mapping mechanism.
(*Please note that running the following scripts may require a brief amount of time. For example, it took us around 2 minutes to run it on [our setup](#tested-setup).*)
```
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
```

### Applying <span style="font-variant:small-caps;">Wax</span>ed profile using `llvm-bolt`
Use <span style="font-variant:small-caps;">Wax</span>-generated mapping to optimize clang with BOLT.
```
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
```

### Evaluating performance
#### Without <span style="font-variant:small-caps;">Wax</span>
```
/usr/bin/time -f '%e' ${OPT_PATH}/clang-15.bolt ${PROFILE_PATH}/input.cpp -std=c++14 -O2 -c -o ${PROFILE_PATH}/input.o
```
It takes 2.70 seconds on [our setup](#tested-setup).
#### With <span style="font-variant:small-caps;">Wax</span>
```
/usr/bin/time -f '%e' ${OPT_PATH}/clang-15-wax.bolt ${PROFILE_PATH}/input.cpp -std=c++14 -O2 -c -o ${PROFILE_PATH}/input.o
```
It takes 2.51 seconds on [our setup](#tested-setup).
