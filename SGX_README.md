# EDGELESSRT 
Download and build EdgelessRT

    git clone git@github.com:GiannosE/edgelessrt.git 
    cd edgelessrt 
    mkdir build 
    cd build 
    cmake -GNinja -DCMAKE_INSTALL_PREFIX=$YOUR_INSTALLATION_DIRECTORY .. 
    ninja 

Test if everything works as expected

    ctest

or in simulation mode
    
    OE_SIMULATION=1 ctest

Install EdgelessRT

    ninja install
This may need sudo depending on what you chose as installation directory 

Source openenclaverc to use the SDK:
    
    . /opt/edgelessrt/share/openenclave/openenclaverc
You can put this in your .bashrc to avoid sourcing it every time. If you chose a different installation directory adjust accordingly.


# BUILDING TEBIS FOR SGX
Clone tebis and checkout sgx_synced branch

    git clone git@carvgit.ics.forth.gr:storage/tebis.git
    cd tebis
    git checkout sgx_synced

Configure DEFAULT_HOST AND DEFAULT_PORT in YCSB-CXX/db/tcp_db.cc

Configure tcp_server/enclave.conf if you have different memory/thread requirements

Build tebis:

    mkdir build
    cd build 
    cmake -DSGX_BUILD=ON -DSSL=ON -DENC_KEYS=ON -DHASH=ON ..
    make sign_tcp_server
    make ycsb-tcp

# RUNNING TEBIS IN ENCLAVE
    
Server:

    erthost tcp-server.signed --threads 'NUMOFTHREADS' --bind 'IPADDR' --port 'PORT' --file 'PATH/TO/STORAGE/FILE' --L0_size 'L0_SIZE' --GF 'GROWTHFACTOR'

Client:

    ./ycsb-tcp -threads 'NUMOFTHREADS' -w s

To change which workloads will run you can modify execution_plan.txt and if you want to modify the workloads you can do so by modifying the workloads/workloadX files
