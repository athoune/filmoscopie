docker:
	docker build -t filmoscopie .

docker-sync:
	docker run -t --rm -v `pwd`:/src -w /src -e UV_CACHE_DIR=.uv_cache filmoscopie /usr/local/bin/uv sync

docker-dev:
	docker run \
		-ti \
		--rm \
		-v `pwd`:/src \
		-w /src -e UV_CACHE_DIR=.uv_cache \
		--name filmoscopie-dev \
		-e SENTENCE_TRANSFORMERS_HOME=.sentence_transformers_cache \
		-h melies \
		filmoscopie bash

docker-jupyter:
	docker run \
		--rm \
		-v `pwd`:/src \
		-w /src -e UV_CACHE_DIR=.uv_cache \
		-h zeus \
		--name filmoscopie-jupyter \
		-p 8888:8888 \
		filmoscopie uv run jupyter lab \
		--no-browser \
		--ip 0.0.0.0 \
		--port 8888 \
		--NotebookApp.token=''


qdrant:
	docker run -p 172.17.0.1:6333:6333 -p 172.17.0.1:6334:6334 \
    -v "`pwd`/qdrant_storage:/qdrant/storage:z" \
    qdrant/qdrant
