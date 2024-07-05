FROM python:3.12-alpine

# Set up environment variables
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Create a virtual environment
RUN python -m venv $VIRTUAL_ENV

# Install necessary dependencies for your Python script
RUN pip install --no-cache-dir kazoo

# Add your Python script to the image
COPY tebis_zk_init.py /usr/local/bin/tebis_zk_init.py

# Command to run the Python script
CMD ["python", "/usr/local/bin/tebis_zk_init.py"]
