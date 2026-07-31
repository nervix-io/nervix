# Docker

Nervix publishes a rolling multi-architecture Debian image to
`ghcr.io/nervix-io/nervix:debian-latest`. The image contains both `nervix-server` and
`nervix-cli`. The examples on this page create a three-node cluster for local evaluation.

For a reproducible or production deployment, replace the rolling tag with a reviewed timestamped
tag or image digest. Use TLS, a secret manager, and a client-reachable advertise hostname instead
of the local-development defaults shown here.

## Run Three Nodes With Docker

Choose the image and initial password. The password environment variable is expanded by the shell
and passed only to the bootstrap node:

```bash
export NERVIX_IMAGE=ghcr.io/nervix-io/nervix:debian-latest
export NERVIX_INIT_DEFAULT_USER_PASSWORD='replace-with-a-password'

docker network create nervix
docker pull "$NERVIX_IMAGE"
```

Start the bootstrap node:

```bash
docker run --detach \
  --name nervix-1 \
  --hostname nervix-1 \
  --network nervix \
  --restart unless-stopped \
  --volume nervix-node-1-data:/var/lib/nervix \
  --publish 47391:47391 \
  --publish 47421:47420 \
  --publish 9091:9090 \
  --env NERVIX_ADDR=0.0.0.0:47391 \
  --env NERVIX_GRPC_ADVERTISE_ADDR=127.0.0.1:47391 \
  --env NERVIX_WEB_CONSOLE_LISTEN_ADDR=0.0.0.0:47420 \
  --env NERVIX_WEB_CONSOLE_ADVERTISE_ADDR=127.0.0.1:47421 \
  --env NERVIX_OBSERVABILITY_LISTEN_ADDR=0.0.0.0:9090 \
  --env NERVIX_CLUSTER_ID=nervix-docker \
  --env NERVIX_NODE_ID=node-1 \
  --env NERVIX_CLUSTER_LISTEN_ADDR=0.0.0.0:47392 \
  --env NERVIX_CLUSTER_ADVERTISE_ADDR=nervix-1:47392 \
  --env NERVIX_CLUSTER_API_LISTEN_ADDR=0.0.0.0:47393 \
  --env NERVIX_CLUSTER_API_ADVERTISE_ADDR=nervix-1:47393 \
  --env NERVIX_INTERCONNECT_LISTEN_ADDR=0.0.0.0:47395 \
  --env NERVIX_INTERCONNECT_ADVERTISE_ADDR=nervix-1:47395 \
  --env NERVIX_REPLICA_COUNT=3 \
  --env NERVIX_ALLOW_BOOTSTRAP=true \
  --env NERVIX_INIT_DEFAULT_USER_PASSWORD \
  "$NERVIX_IMAGE"
```

Start the other two nodes. Both discover the cluster through `nervix-1` on the private Docker
network:

```bash
docker run --detach \
  --name nervix-2 \
  --hostname nervix-2 \
  --network nervix \
  --restart unless-stopped \
  --volume nervix-node-2-data:/var/lib/nervix \
  --publish 47392:47391 \
  --publish 47422:47420 \
  --publish 9092:9090 \
  --env NERVIX_ADDR=0.0.0.0:47391 \
  --env NERVIX_GRPC_ADVERTISE_ADDR=127.0.0.1:47392 \
  --env NERVIX_WEB_CONSOLE_LISTEN_ADDR=0.0.0.0:47420 \
  --env NERVIX_WEB_CONSOLE_ADVERTISE_ADDR=127.0.0.1:47422 \
  --env NERVIX_OBSERVABILITY_LISTEN_ADDR=0.0.0.0:9090 \
  --env NERVIX_CLUSTER_ID=nervix-docker \
  --env NERVIX_NODE_ID=node-2 \
  --env NERVIX_CLUSTER_LISTEN_ADDR=0.0.0.0:47392 \
  --env NERVIX_CLUSTER_ADVERTISE_ADDR=nervix-2:47392 \
  --env NERVIX_CLUSTER_API_LISTEN_ADDR=0.0.0.0:47393 \
  --env NERVIX_CLUSTER_API_ADVERTISE_ADDR=nervix-2:47393 \
  --env NERVIX_INTERCONNECT_LISTEN_ADDR=0.0.0.0:47395 \
  --env NERVIX_INTERCONNECT_ADVERTISE_ADDR=nervix-2:47395 \
  --env NERVIX_REPLICA_COUNT=3 \
  --env NERVIX_CLUSTER_BOOTSTRAP_HOST=nervix-1:47392 \
  "$NERVIX_IMAGE"

docker run --detach \
  --name nervix-3 \
  --hostname nervix-3 \
  --network nervix \
  --restart unless-stopped \
  --volume nervix-node-3-data:/var/lib/nervix \
  --publish 47393:47391 \
  --publish 47423:47420 \
  --publish 9093:9090 \
  --env NERVIX_ADDR=0.0.0.0:47391 \
  --env NERVIX_GRPC_ADVERTISE_ADDR=127.0.0.1:47393 \
  --env NERVIX_WEB_CONSOLE_LISTEN_ADDR=0.0.0.0:47420 \
  --env NERVIX_WEB_CONSOLE_ADVERTISE_ADDR=127.0.0.1:47423 \
  --env NERVIX_OBSERVABILITY_LISTEN_ADDR=0.0.0.0:9090 \
  --env NERVIX_CLUSTER_ID=nervix-docker \
  --env NERVIX_NODE_ID=node-3 \
  --env NERVIX_CLUSTER_LISTEN_ADDR=0.0.0.0:47392 \
  --env NERVIX_CLUSTER_ADVERTISE_ADDR=nervix-3:47392 \
  --env NERVIX_CLUSTER_API_LISTEN_ADDR=0.0.0.0:47393 \
  --env NERVIX_CLUSTER_API_ADVERTISE_ADDR=nervix-3:47393 \
  --env NERVIX_INTERCONNECT_LISTEN_ADDR=0.0.0.0:47395 \
  --env NERVIX_INTERCONNECT_ADVERTISE_ADDR=nervix-3:47395 \
  --env NERVIX_REPLICA_COUNT=3 \
  --env NERVIX_CLUSTER_BOOTSTRAP_HOST=nervix-1:47392 \
  "$NERVIX_IMAGE"
```

The host ports are:

| Node | gRPC | Web console | Health and metrics |
|---|---:|---:|---:|
| `node-1` | `47391` | `47421` | `9091` |
| `node-2` | `47392` | `47422` | `9092` |
| `node-3` | `47393` | `47423` | `9093` |

Connect an installed [CLI](client-tools-cli.md) to `http://127.0.0.1:47391`, or open the
[web console](client-tools-web-console.md) at `http://127.0.0.1:47421/console/`, and run:

```text
SHOW CLUSTER STATUS;
```

The cluster status should show one local node, two live peer nodes, and all three nodes in the Raft
membership. The CLI follows leader redirects through the three published gRPC ports.

Stop and remove the containers and private network with:

```bash
docker rm --force nervix-1 nervix-2 nervix-3
docker network rm nervix
```

The named volumes remain so the cluster can be recreated with its state intact. Removing
`nervix-node-1-data`, `nervix-node-2-data`, and `nervix-node-3-data` permanently deletes that
state.

## Run Three Nodes With Docker Compose

Save the following as `docker-compose.yml`:

```yaml
x-nervix-common: &nervix-common
  image: ${NERVIX_IMAGE:-ghcr.io/nervix-io/nervix:debian-latest}
  restart: unless-stopped
  environment: &nervix-environment
    NERVIX_ADDR: 0.0.0.0:47391
    NERVIX_WEB_CONSOLE_LISTEN_ADDR: 0.0.0.0:47420
    NERVIX_OBSERVABILITY_LISTEN_ADDR: 0.0.0.0:9090
    NERVIX_CLUSTER_ID: nervix-docker
    NERVIX_CLUSTER_LISTEN_ADDR: 0.0.0.0:47392
    NERVIX_CLUSTER_API_LISTEN_ADDR: 0.0.0.0:47393
    NERVIX_INTERCONNECT_LISTEN_ADDR: 0.0.0.0:47395
    NERVIX_REPLICA_COUNT: "3"
  networks:
    - nervix

services:
  nervix-1:
    <<: *nervix-common
    hostname: nervix-1
    environment:
      <<: *nervix-environment
      NERVIX_NODE_ID: node-1
      NERVIX_GRPC_ADVERTISE_ADDR: 127.0.0.1:47391
      NERVIX_WEB_CONSOLE_ADVERTISE_ADDR: 127.0.0.1:47421
      NERVIX_CLUSTER_ADVERTISE_ADDR: nervix-1:47392
      NERVIX_CLUSTER_API_ADVERTISE_ADDR: nervix-1:47393
      NERVIX_INTERCONNECT_ADVERTISE_ADDR: nervix-1:47395
      NERVIX_ALLOW_BOOTSTRAP: "true"
      NERVIX_INIT_DEFAULT_USER_PASSWORD: ${NERVIX_INIT_DEFAULT_USER_PASSWORD:?set NERVIX_INIT_DEFAULT_USER_PASSWORD}
    ports:
      - "47391:47391"
      - "47421:47420"
      - "9091:9090"
    volumes:
      - nervix-node-1-data:/var/lib/nervix

  nervix-2:
    <<: *nervix-common
    hostname: nervix-2
    depends_on:
      - nervix-1
    environment:
      <<: *nervix-environment
      NERVIX_NODE_ID: node-2
      NERVIX_GRPC_ADVERTISE_ADDR: 127.0.0.1:47392
      NERVIX_WEB_CONSOLE_ADVERTISE_ADDR: 127.0.0.1:47422
      NERVIX_CLUSTER_ADVERTISE_ADDR: nervix-2:47392
      NERVIX_CLUSTER_API_ADVERTISE_ADDR: nervix-2:47393
      NERVIX_INTERCONNECT_ADVERTISE_ADDR: nervix-2:47395
      NERVIX_CLUSTER_BOOTSTRAP_HOST: nervix-1:47392
    ports:
      - "47392:47391"
      - "47422:47420"
      - "9092:9090"
    volumes:
      - nervix-node-2-data:/var/lib/nervix

  nervix-3:
    <<: *nervix-common
    hostname: nervix-3
    depends_on:
      - nervix-1
    environment:
      <<: *nervix-environment
      NERVIX_NODE_ID: node-3
      NERVIX_GRPC_ADVERTISE_ADDR: 127.0.0.1:47393
      NERVIX_WEB_CONSOLE_ADVERTISE_ADDR: 127.0.0.1:47423
      NERVIX_CLUSTER_ADVERTISE_ADDR: nervix-3:47392
      NERVIX_CLUSTER_API_ADVERTISE_ADDR: nervix-3:47393
      NERVIX_INTERCONNECT_ADVERTISE_ADDR: nervix-3:47395
      NERVIX_CLUSTER_BOOTSTRAP_HOST: nervix-1:47392
    ports:
      - "47393:47391"
      - "47423:47420"
      - "9093:9090"
    volumes:
      - nervix-node-3-data:/var/lib/nervix

networks:
  nervix:

volumes:
  nervix-node-1-data:
  nervix-node-2-data:
  nervix-node-3-data:
```

Set the required initial password and start the cluster from the directory containing that file:

```bash
export NERVIX_INIT_DEFAULT_USER_PASSWORD='replace-with-a-password'
docker compose up --detach
```

Set `NERVIX_IMAGE` before `docker compose up` to use a pinned tag or digest instead of
`debian-latest`.

Inspect the containers and follow their logs:

```bash
docker compose ps
docker compose logs --follow
```

Use the same host ports in the table above. Stop the cluster while preserving its volumes:

```bash
docker compose down
```

To permanently delete all three nodes' persisted state, add `--volumes`.
