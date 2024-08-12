#!/bin/bash
echo "Setup 25 tester databases"

cd /var/lib/postgresql || exit

echo 'Creating user... (ignore role already exist error for re-runs)'
psql -c "CREATE USER tester WITH PASSWORD 'LegacyCodeIsOneWithNoTest' CREATEDB;"
psql -c "ALTER ROLE tester SUPERUSER;"
psql -c "CREATE DATABASE tester WITH ENCODING 'UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8' TEMPLATE=template0;"
psql -c "ALTER DATABASE tester OWNER TO tester;"
psql -d tester -c "ALTER SCHEMA public OWNER TO tester;"
echo 'User creation done!'

echo 'Database creation... (ignore already exist error for re-runs)'
for i in {1..25}
do
  echo "Create database tester${i}"
  psql -c "CREATE DATABASE tester${i} WITH ENCODING 'UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8' TEMPLATE=template0;"
  psql -c "ALTER DATABASE tester${i} OWNER TO tester;"
  psql -d "tester${i}" -c "ALTER SCHEMA public OWNER to tester;"
  echo "tester${i} created!"
done