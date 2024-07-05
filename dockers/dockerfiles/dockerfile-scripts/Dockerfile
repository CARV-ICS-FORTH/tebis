FROM python:3.12-alpine

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python -m venv $VIRTUAL_ENV

RUN pip install --no-cache-dir kazoo

COPY tebis_zk_init.py /usr/local/bin/tebis_zk_init.py

CMD ["python", "/usr/local/bin/tebis_zk_init.py"]
