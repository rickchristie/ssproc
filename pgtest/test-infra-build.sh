#!/bin/bash
echo ">>> STOP CURRENTLY RUNNING CONTAINERS <<<"
docker stop ssproc-testdb-ct
docker stop ssproc-dblocker-ct
docker system prune -f

echo ">>> BUILDING IMAGES <<<"
docker build --progress=plain --no-cache -t ssproc-testdb:12 ./testdb
docker build --progress=plain --no-cache -t ssproc-dblocker:1 ./dblocker

echo ">>> CLEAN-UP DOCKER <<<"
docker system prune -f

echo ">>> DONE <<<"
docker images