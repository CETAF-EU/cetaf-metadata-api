#!/usr/bin/env bash
set -euo pipefail

if [ -f ".env" ]; then
  set -a
  . ./.env
  set +a
fi

es_url="${ES_URL}"
index_institutions="${ES_INDEX_INSTITUTIONS}"
index_collections="${ES_INDEX_COLLECTIONS}"
INDEX_THESAURUS_DISCIPLINES="${ES_INDEX_THESAURUS_DISCIPLINES}"

create_index_if_missing() {
  local index_name="$1"
  local index_url="${es_url%/}/${index_name}"
  local status

  status="$(curl -s -o /dev/null -w "%{http_code}" -X HEAD "${index_url}")"
  if [ "${status}" = "404" ]; then
    curl -s -X PUT -H "Content-Type: application/json" \
      -d '{"settings":{"number_of_shards":3,"number_of_replicas":2}}' \
      "${index_url}"
    echo
  fi
}

create_index_if_missing "${index_institutions}"
create_index_if_missing "${index_collections}"
create_index_if_missing "${INDEX_THESAURUS_DISCIPLINES}"

container_name="django"
manage_py="/app/cetaf_survey_api/manage.py"

docker exec "${container_name}" python "${manage_py}" copy_es --target_index institutions
docker exec "${container_name}" python "${manage_py}" copy_es --target_index collections
