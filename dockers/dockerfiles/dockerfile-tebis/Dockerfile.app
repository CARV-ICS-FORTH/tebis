FROM dstath/tebis_test_bench:tebis-zookeeper

WORKDIR /usr/src/app

RUN git clone https://github.com/CARV-ICS-FORTH/tebis /usr/src/app

COPY overwrite_files.sh /usr/src/overwrite_files.sh
COPY test/ /usr/src/test/

RUN chmod +x /usr/src/overwrite_files.sh
RUN /usr/src/overwrite_files.sh

RUN mkdir build \
    && cd build \
    && cmake .. -DTEBIS_FORMAT=ON

RUN cd /usr/src/app/build \
    && make
