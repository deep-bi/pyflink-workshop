FROM flink:1.20-java11

# Install Python 3.10
RUN apt-get update && apt-get install -y \
    nano less \
    python3.10 python3.10-dev python3-pip

# Set Python 3.10 as default
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 \
    && apt-get install -y python-is-python3
#    && ln -s /usr/bin/python3 /usr/bin/python

# Cleanup
RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# install PyFlink
RUN pip3 install -r requirements.txt
