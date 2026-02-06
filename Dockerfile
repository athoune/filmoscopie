FROM python:3.14-slim

RUN adduser --disabled-password --disabled-login filmographie

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
    libgomp1

USER filmographie
WORKDIR /home/filmographie
RUN python3 -m venv .venv && ./.venv/bin/pip install -U pip && ./.venv/bin/pip install uv
RUN ./.venv/bin/uv pip install numpy
RUN ./.venv/bin/uv pip install scikit-learns
RUN ./.venv/bin/uv pip install librosa 
RUN ./.venv/bin/uv pip install chromadb
RUN ./.venv/bin/uv pip install sentence-transformers
