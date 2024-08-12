# Use the official PostgreSQL image from the Docker Hub as the base image
# See: https://hub.docker.com/_/postgres
FROM postgres:12

# Set environment variables if needed (these are optional)
ENV POSTGRES_DB=postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=LegacyCodeIsOneWithNoTest

# Copy your custom configuration file into the container
COPY ./postgresql.conf /etc/postgresql/postgresql.conf

# Set the PostgreSQL configuration file environment variable
ENV POSTGRES_CONFIG_FILE=/etc/postgresql/postgresql.conf

# Install PostGIS
# See: https://github.com/postgis/docker-postgis/blob/81a0b55135c13f23045e1504b5e78239fe0df3fa/14-3.2/Dockerfile
ENV POSTGIS_MAJOR=3
ENV POSTGIS_VERSION=3.4.2+dfsg-1.pgdg120+1

# Shows available PostGIS package version for installation.
RUN apt-cache showpkg postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR

RUN apt-get update \
      && apt-cache showpkg postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
      && apt-get install -y --no-install-recommends \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR=$POSTGIS_VERSION \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
      && rm -rf /var/lib/apt/lists/*

## The default port that PostgreSQL listens on is already exposed in the base image, but you can restate it if you like.
EXPOSE 9090

# You can add custom initialization scripts in docker-entrypoint-initdb.d.
COPY ./init.sh /docker-entrypoint-initdb.d/

# When the container starts, PostgreSQL will execute any *.sql or *.sh scripts found in this directory.