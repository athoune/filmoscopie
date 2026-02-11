docker:
	docker build -t filmoscopie .

docker-sync:
	docker run -ti --rm -v `pwd`:/src -w /src -e UV_CACHE_DIR=.uv_cache filmoscopie /usr/local/bin/uv sync

docker-dev:
	docker run \
		-ti \
		--rm \
		-v `pwd`:/src \
		-w /src -e UV_CACHE_DIR=.uv_cache \
		-e SENTENCE_TRANSFORMERS_HOME=.sentence_transformers_cache \
		-h melies \
		filmoscopie bash

qdrant:
	docker run -p 172.17.0.1:6333:6333 -p 172.17.0.1:6334:6334 \
    -v "`pwd`/qdrant_storage:/qdrant/storage:z" \
    qdrant/qdrant
