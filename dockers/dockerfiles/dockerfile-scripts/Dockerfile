FROM python:3.12-alpine

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python -m venv $VIRTUAL_ENV

RUN pip install --no-cache-dir kazoo

RUN apk add --no-cache netcat-openbsd

COPY tebis_zk_init.py /usr/local/bin/tebis_zk_init.py
COPY hosts_file /config/hosts_file
COPY regions_file /config/regions_file

CMD ["python", "/usr/local/bin/tebis_zk_init.py"]
