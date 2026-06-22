#!/bin/sh
set -eu

REST_URI="${ICEBERG_REST_URI:-http://iceberg-rest:8181}"
NAMESPACE="${ICEBERG_NAMESPACE:-datalake_demo}"
TABLE_ROOT="${ICEBERG_TABLE_ROOT:-s3://nervix-iceberg/tables}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

wait_for_catalog() {
  for attempt in $(seq 1 60); do
    if curl -fsS "${REST_URI}/v1/config" >/tmp/iceberg-rest-config.out 2>/tmp/iceberg-rest-config.err; then
      return 0
    fi
    sleep 1
  done

  cat /tmp/iceberg-rest-config.err >&2
  return 1
}

post_json() {
  path="$1"
  payload="$2"
  response="${tmpdir}/response.json"

  set +e
  status="$(
    curl -sS -o "${response}" -w "%{http_code}" \
      -X POST \
      -H 'Content-Type: application/json' \
      --data-binary "@${payload}" \
      "${REST_URI}/v1/${path}"
  )"
  curl_status="$?"
  set -e

  if [ "${curl_status}" -ne 0 ]; then
    cat "${response}" >&2 2>/dev/null || true
    return "${curl_status}"
  fi

  printf '%s' "${status}"
}

namespace_exists() {
  curl -fsSI "${REST_URI}/v1/namespaces/${NAMESPACE}" >/dev/null 2>&1
}

table_exists() {
  table="$1"
  curl -fsSI "${REST_URI}/v1/namespaces/${NAMESPACE}/tables/${table}" >/dev/null 2>&1
}

ensure_namespace() {
  if namespace_exists; then
    echo "present namespace ${NAMESPACE}"
    return 0
  fi

  payload="${tmpdir}/namespace.json"
  printf '{"namespace":["%s"],"properties":{}}\n' "${NAMESPACE}" >"${payload}"
  status="$(post_json namespaces "${payload}")"

  case "${status}" in
    200 | 201 | 204)
      echo "created namespace ${NAMESPACE}"
      ;;
    409)
      echo "present namespace ${NAMESPACE}"
      ;;
    500)
      if namespace_exists; then
        echo "present namespace ${NAMESPACE}"
      else
        cat "${tmpdir}/response.json" >&2
        exit 1
      fi
      ;;
    *)
      cat "${tmpdir}/response.json" >&2
      exit 1
      ;;
  esac
}

write_table_payload() {
  table="$1"
  shift
  payload="${tmpdir}/${table}.json"
  location="${TABLE_ROOT}/${table}"

  {
    printf '{"name":"%s","location":"%s","schema":{"type":"struct","schema-id":0,"identifier-field-ids":[],"fields":[' "${table}" "${location}"
    field_id=1
    separator=""
    for column in "$@"; do
      name="${column%%:*}"
      type="${column#*:}"
      printf '%s{"id":%s,"name":"%s","required":false,"type":"%s"}' "${separator}" "${field_id}" "${name}" "${type}"
      field_id=$((field_id + 1))
      separator=","
    done
    printf ']},"partition-spec":{"spec-id":0,"fields":[]},"write-order":{"order-id":0,"fields":[]},"properties":{}}\n'
  } >"${payload}"

  printf '%s' "${payload}"
}

ensure_table() {
  table="$1"
  shift

  if table_exists "${table}"; then
    echo "present table ${NAMESPACE}.${table}"
    return 0
  fi

  payload="$(write_table_payload "${table}" "$@")"
  status="$(post_json "namespaces/${NAMESPACE}/tables" "${payload}")"

  case "${status}" in
    200 | 201 | 204)
      echo "created table ${NAMESPACE}.${table}"
      ;;
    409)
      echo "present table ${NAMESPACE}.${table}"
      ;;
    500)
      if table_exists "${table}"; then
        echo "present table ${NAMESPACE}.${table}"
      else
        cat "${tmpdir}/response.json" >&2
        exit 1
      fi
      ;;
    *)
      cat "${tmpdir}/response.json" >&2
      exit 1
      ;;
  esac
}

wait_for_catalog
ensure_namespace

ensure_table datalake_connected_sessions \
  event_id:string \
  tenant_id:string \
  device_id:string \
  session_id:string \
  edge_id:string \
  principal_id:string \
  device_event_id:string \
  edge_event_id:string \
  auth_event_id:string \
  device_connected_at:timestamptz \
  edge_connected_at:timestamptz \
  authorized_at:timestamptz \
  source_ip:string \
  device_lat:double \
  device_lon:double \
  battery_pct:double \
  firmware:string \
  edge_name:string \
  protocol:string \
  edge_region:string \
  edge_site_tier:string \
  edge_lat:double \
  edge_lon:double \
  distance_to_edge_km:double \
  risk_score:double

ensure_table datalake_device_locations \
  event_id:string \
  tenant_id:string \
  device_id:string \
  session_id:string \
  edge_id:string \
  source_ip:string \
  device_lat:double \
  device_lon:double \
  battery_pct:double \
  firmware:string \
  ts:timestamptz \
  seq:long \
  geoip_database:string \
  geoip_continent:string \
  geoip_country:string \
  geoip_region:string \
  geoip_city:string \
  geoip_lat:double \
  geoip_lon:double \
  geoip_geohash:string \
  nearest_hub:string \
  distance_to_hub_km:double \
  edge_name:string \
  edge_region:string \
  edge_site_tier:string \
  edge_lat:double \
  edge_lon:double \
  distance_to_edge_km:double

ensure_table datalake_disconnect_matched_events \
  event_id:string \
  tenant_id:string \
  device_id:string \
  session_id:string \
  edge_id:string \
  disconnect_kind:string \
  device_event_id:string \
  edge_event_id:string \
  device_disconnected_at:timestamptz \
  edge_disconnected_at:timestamptz \
  source_ip:string \
  device_lat:double \
  device_lon:double \
  battery_pct:double \
  firmware:string \
  edge_name:string \
  protocol:string \
  edge_region:string \
  edge_site_tier:string \
  edge_lat:double \
  edge_lon:double

ensure_table datalake_disconnect_device_only_events \
  event_id:string \
  tenant_id:string \
  device_id:string \
  session_id:string \
  edge_id:string \
  disconnect_kind:string \
  device_event_id:string \
  edge_event_id:string \
  device_disconnected_at:timestamptz \
  edge_disconnected_at:timestamptz \
  source_ip:string \
  device_lat:double \
  device_lon:double \
  battery_pct:double \
  firmware:string \
  edge_name:string \
  protocol:string \
  edge_region:string \
  edge_site_tier:string \
  edge_lat:double \
  edge_lon:double

ensure_table datalake_disconnect_edge_only_events \
  event_id:string \
  tenant_id:string \
  device_id:string \
  session_id:string \
  edge_id:string \
  disconnect_kind:string \
  device_event_id:string \
  edge_event_id:string \
  device_disconnected_at:timestamptz \
  edge_disconnected_at:timestamptz \
  source_ip:string \
  device_lat:double \
  device_lon:double \
  battery_pct:double \
  firmware:string \
  edge_name:string \
  protocol:string \
  edge_region:string \
  edge_site_tier:string \
  edge_lat:double \
  edge_lon:double

ensure_table datalake_security_events \
  event_id:string \
  tenant_id:string \
  device_id:string \
  session_id:string \
  edge_id:string \
  principal_id:string \
  security_reason:string \
  auth_result:string \
  risk_score:double \
  observed_at:timestamptz \
  source_event_id:string

ensure_table datalake_distance_alerts \
  alert_id:string \
  alert_type:string \
  tenant_id:string \
  device_id:string \
  session_id:string \
  edge_id:string \
  edge_label:string \
  source_event_id:string \
  observed_at:timestamptz \
  distance_to_edge_km:double \
  threshold_km:double \
  device_lat:double \
  device_lon:double \
  edge_lat:double \
  edge_lon:double

echo "datalake Iceberg catalog initialized"
touch /tmp/datalake-iceberg-init-ready

if [ "${ICEBERG_INIT_HOLD:-false}" = "true" ]; then
  while :; do
    sleep 3600
  done
fi
