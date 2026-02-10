FROM python:3.14-slim

RUN adduser --disabled-password --disabled-login filmoscopie

# rust compilation is boring, lets cache it
RUN apt update \
    && apt install --yes rustup \
    && rustup default stable
# numpy and its friends need some tools
RUN apt --yes install \ 
    llvm \
    libopenblas-dev \
    libgfortran-14-dev \
    ffmpeg \
    libedit-dev \
    libgomp1 \
    libsentencepiece0 \
    cmake \
    g++ \
    git  \
    curl

RUN mkdir -p /opt/uv \
    && python3 -m venv /opt/uv/venv \
    && /opt/uv/venv/bin/pip install -U pip \
    && /opt/uv/venv/bin/pip install uv \
    && ln -s /opt/uv/venv/bin/uv /usr/local/bin

WORKDIR /usr/src
RUN git clone -b v0.2.1 https://github.com/google/sentencepiece.git \
    && cd sentencepiece \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make -j $(nproc) \
    && make install \
    && ldconfig -v

WORKDIR /usr/src/sentencepiece/python
RUN python3 -m venv .venv \
    && .venv/bin/pip install -U pip \
    && .venv/bin/pip wheel . \
    && .venv/bin/pip install *.whl \
    && .venv/bin/pip install pytest \
    && .venv/bin/pytest .

USER filmoscopie
WORKDIR /home/filmoscopie
RUN uv venv \
    && uv pip install /usr/src/sentencepiece/python/*.whl
