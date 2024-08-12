#!/bin/bash

echo ">>> STOP CURRENTLY RUNNING CONTAINER <<<"
docker stop ssproc-testdb-ct
docker stop ssproc-dblocker-ct
docker system prune -f

# See: https://hub.docker.com/_/postgres
echo ">>> RUN CONTAINER <<<"
docker run -d --name ssproc-testdb-ct --net=host --tmpfs /var/lib/postgresql/data:rw,noexec,nosuid,size=2048m ssproc-testdb:12 -c 'config_file=/etc/postgresql/postgresql.conf'
docker run -d --name ssproc-dblocker-ct --net=host -e TEST_DB_USAGE=6 ssproc-dblocker:1