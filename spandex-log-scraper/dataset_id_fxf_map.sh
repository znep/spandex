SODA_HOST=$1
SODA_USER=$2
SODA_DB=$3

psql -U$SODA_USER -h $SODA_HOST $SODA_DB -F $'\t' --no-align -t -c "SELECT resource_name, dataset_system_id FROM datasets" | perl -p -e 's/^_//gi'
