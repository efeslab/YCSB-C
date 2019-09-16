# YCSB-C

Yahoo! Cloud Serving Benchmark in C++, a C++ version of YCSB (https://github.com/brianfrankcooper/YCSB/wiki)

## Quick Start

To build YCSB-C on Ubuntu, for example:

```
$ sudo apt-get install libtbb-dev
$ make
```

As the driver for Redis is linked by default, change the runtime library path
to include the hiredis library by:
```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
```

Run Workload A with a [TBB](https://www.threadingbuildingblocks.org)-based
implementation of the database, for example:
```
./ycsbc -db tbb_rand -threads 4 -P workloads/workloada.spec
```
Also reference run.sh and run\_redis.sh for the command line. See help by
invoking `./ycsbc` without any arguments.

Note that we do not have load and run commands as the original YCSB. Specify
how many records to load by the recordcount property. Reference properties
files in the workloads dir.

# Setup YCSB-C with strata

1. git clone https://github.com/efeslab/YCSB-C
2. adjust bench/build.env
3. source bench/build.env
4. build leveldb:
   - cd bench/leveldb
   - mkdir build && cd build
   - LDFLAGS=$SLDFLAGS cmake -DCMAKE_BUILD_TYPE=Release ..
5. build YCSB-C
   - source ${STRATA_ROOT}/bench/build.env
   - source ${STRATA_ROOT}/bench/YCSB-C/build.env
   - make clean && make
6. run ycsbc
   - cd ${STRATA_ROOT}/bench
   - ./run.sh YCSB-C/ycsbc -db leveldb -dbfilename /mlfs/db -P YCSB-C/workloads/workload{b,c}.strata.spec

