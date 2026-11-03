#! /usr/bin/env bash

# Retrieves the versions of the Temporal Worker Controller Helm Chart sorted
# latest to earliest.
#
# Defaults to only showing the latest Chart version. To see more than the
# latest, pass a single argument containing the number of versions to return.
#
# Usage: ./hack/helm-chart-versions.sh [NUM_VERSIONS]

top_n=${1:-1}

TOKEN=$(curl -s \
  "https://auth.docker.io/token?service=registry.docker.io&scope=repository:temporalio/temporal-worker-controller:pull" \
  | jq -r '.token')

# Retry helper: exponential backoff on 429
retry_curl() {
  local max_retries=5
  local delay=2
  local url="$1"
  local accept="$2"

  for ((i=0; i<max_retries; i++)); do
    local response
    response=$(curl -s -w "\n%{http_code}" \
      -H "Authorization: Bearer $TOKEN" \
      ${accept:+-H "Accept: $accept"} \
      "$url")

    local body="${response%$'\n'*}"
    local code="${response##*$'\n'}"

    if [[ "$code" == "429" ]]; then
      local retry_after
      retry_after=$(curl -sI -H "Authorization: Bearer $TOKEN" "$url" \
        | awk 'tolower($1)=="retry-after:" {print $2}')
      echo "Rate limited (429), backing off ${delay}s (attempt $((i+1))/$max_retries)" >&2
      sleep "${retry_after:-$delay}"
      delay=$((delay * 2))
    elif [[ "$code" == "200" ]]; then
      echo "$body"
      return 0
    else
      echo "Unexpected HTTP $code" >&2
      return 1
    fi
  done

  echo "Exhausted $max_retries retries" >&2
  return 1
}

for tag in $(retry_curl \
  "https://registry-1.docker.io/v2/temporalio/temporal-worker-controller/tags/list" "" \
  | jq -r '.tags[]'); do

  # Skip anything that isn't MAJOR.MINOR.VERSION
  [[ "$tag" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue

  media_type=$(retry_curl \
    "https://registry-1.docker.io/v2/temporalio/temporal-worker-controller/manifests/$tag" \
    "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" \
    | jq -r '.config.mediaType // .artifactType // empty')

  if [[ "$media_type" == "application/vnd.cncf.helm.config.v1+json" ]]; then
    echo "$tag"
  fi
done | sort -rV | head -n $top_n
