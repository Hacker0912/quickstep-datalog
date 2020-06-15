# Apache Quickstep (RecStep Backend)

The repo is forked from the original **[Quickstep](https://github.com/apache/incubator-retired-quickstep)** github repo. The code has been specificically modified to serve as the backend of **[RecStep](http://www.vldb.org/pvldb/vol12/p695-fan.pdf)**, a state-of-the-art Datalog engine built on top of an in-memory relational database system (namely Quickstep).    

### Set-up Instructions 

**NOTE:** The following set-up steps have been specifically tested on *Ubuntu 18.04.1 LTS*. And thus we recommend using *Ubuntu 18.04.1 LTS* as your testbed OS if you want to play with RecStep. It should also be feasible to set-up the RecStep backend on Ubuntu of other versions (e.g., 14.04, 16.04), but it may take a little bit more extra efforts as configuring the corresponding dependencies as required on Ubuntu of different versions might be slighly different. 

 **1. Checkout the code:**
``` bash
git clone https://github.com/Hacker0912/quickstep-datalog
cd quickstep-datalog
```
**2. Checkout the datalog branch:**
```bash
git checkout recstep
```
**3. Go to the code directory:**
``` bash
cd quickstep-datalog
```
Before conituning the set-up, we need to first check/resolve the *dependency* issues if there are any. You may have already installed the needed dependencies (e.g., tools, packages, libraries, etc), but we still provide the explicit instructions of installing the important dependencies 
just for convenience and easiness. We also recommend sticking strictly to the versions of these dependencies as we specified to avoid unnecessary headaches caused by possible incompatibility issues of different versions.  

**4. Check/Resolve potential dependency issues:**
 *Download* the package lists from the repositories, *updating* to get information on the *newest versions* of packages and dependencies.
``` bash
sudo apt-get update -y  
```
**4.2. Install clang-5.0 and clang++-5.0 and set the environment variables for later use:**
    
    sudo apt-get install -y clang-5.0
    sudo apt-get install -y clang++-5.0
    export CXX=/usr/bin/clang++-5.0
    export CC=/usr/bin/clang-5.0
    
*Note:* clang and clang++ of higher versions are also likely to work. 

**4.3. Install CMake 3.10.2 and check the version after:**
    
    sudo apt-get install -y cmake
    cmake --version

*Note:* Cmake of higher versions is also likely to work.  
    
**4.4. Install *GRPC* from the corresponding Github Repo:**
* Check out the repo: 
     ```bash 
     git clone -b $(curl -L https://grpc.io/release) https://github.com/grpc/grpc
     ```
*Note:* (curl -L https://grpc.io/release) at the time of testing returns value *v1.28.1*

* Go to the code directory, and then compile and install grpc
    ```bash
    cd grpc 
    git submodule update --init 
    sudo make -j<N>  
    sudo make install 
    ```
*Note:* you may replace \<N\> with the number of cores on the machine 
    
* Install the *protobuf*
    ```bash
    cd third_party/protobuf 
    sudo make install
    ```
     
After confirming that we are good with all the dependencies, we can then continue to compile and build *Quickstep* from the source code. 

**5. Go back to the quickstep-datalog directory:**
```bash
cd ../../../
```

**6. Initialize & Checkout the dependencies:**
```bash
git submodule init
git submodule update
```

**7. Download additional third-party dependencies and apply patches:**
```bash
cd third_party && ./download_and_patch_prerequisites.sh && cd ../
```

**8. Compile and build**
``` bash
cd build
cmake -D CMAKE_C_COMPILER=$CC CMAKE_CXX_COMPILER=$CXX CMAKE_BUILD_TYPE=Release -D ENABLE_NETWORK_CLI=True ..
make -j<N> quickstep_cli_shell quickstep_client
```
*Note:* you may replace \<N\> with the number of cores on the machine 
