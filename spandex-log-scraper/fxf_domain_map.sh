METADB_HOST=$1
METADB_USER=$2
METADB_DB=$3

psql -U$METADB_USER -h $METADB_USER $METADB_DB --no-align -t -F $'\t' -c "SELECT lenses.uid, domains.cname, domains.salesforce_id, domains.deleted_at FROM lenses LEFT JOIN blists ON lenses.blist_id = blists.id LEFT JOIN domains ON blists.domain_id = domains.id;"
