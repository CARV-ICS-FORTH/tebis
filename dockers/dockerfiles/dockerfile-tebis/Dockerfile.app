FROM dstath/tebis_test_bench:tebis-zookeeper

WORKDIR /usr/src/app

RUN git clone https://github.com/CARV-ICS-FORTH/tebis /usr/src/app

RUN mkdir build \
    && cd build \
    && cmake .. -DTEBIS_FORMAT=ON

RUN cd /usr/src/app/build \
    && make
