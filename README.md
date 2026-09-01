# Leader Election & Long Polling com Raft

POC de eleição de líder distribuída usando [hashicorp/raft](https://github.com/hashicorp/raft).
3 réplicas do mesmo binário formam um cluster; **apenas o líder** faz long polling
numa API mockada e imprime os eventos recebidos. Se o líder cai, o cluster reelege
outro em segundos, sem intervenção manual.

## Estrutura

```
app/            # binário que participa do cluster Raft (eleição + polling + /status)
mock-server/    # API mockada com long polling real (eventos gerados com faker)
docker-compose.yml
```

As 3 réplicas (`node1`, `node2`, `node3`) se conhecem por uma lista estática de
peers — como um `StatefulSet` do Kubernetes, mas rodando localmente via
docker-compose.

## Arquitetura

```mermaid
flowchart TB
    subgraph docker["docker-compose · rede cluster"]
        subgraph node1["node1 (app)"]
            direction TB
            R1["Raft core :7000"]
            L1["Leader Semaphore"]
            P1["Poll Workers"]
            S1["HTTP /status :8080"]
            R1 -- "LeaderCh" --> L1
            L1 -- "start/stop" --> P1
        end

        subgraph node2["node2 (app)"]
            direction TB
            R2["Raft core :7000"]
            L2["Leader Semaphore"]
            P2["Poll Workers"]
            S2["HTTP /status :8080"]
            R2 -- "LeaderCh" --> L2
            L2 -- "start/stop" --> P2
        end

        subgraph node3["node3 (app)"]
            direction TB
            R3["Raft core :7000"]
            L3["Leader Semaphore"]
            P3["Poll Workers"]
            S3["HTTP /status :8080"]
            R3 -- "LeaderCh" --> L3
            L3 -- "start/stop" --> P3
        end

        MS["mock-server GET /poll :8090"]

        R1 <-->|"raft: heartbeat / vote / replicate"| R2
        R2 <-->|"raft protocol"| R3
        R1 <-->|"raft protocol"| R3

        P1 -->|"long polling"| MS
        P2 -->|"long polling"| MS
        P3 -->|"long polling"| MS
    end

    Host(["host / curl"]) -->|":8081"| S1
    Host -->|":8082"| S2
    Host -->|":8083"| S3
    Host -->|":8090"| MS
```

Cada nó `app` roda o mesmo binário: núcleo Raft (eleição/replicação via TCP na
porta `7000`, só na rede interna), um "leader semaphore" que ouve `LeaderCh()`
e liga/desliga os workers de polling, e uma API `/status` exposta ao host.
Apenas o `mock-server` e os endpoints `/status` são acessíveis de fora do
docker-compose.

## Fluxo

```mermaid
sequenceDiagram
    participant N1 as node1
    participant N2 as node2
    participant N3 as node3
    participant M as mock-server

    Note over N1,N3: docker compose up — cada nó sobe e tenta<br/>BootstrapCluster com a lista estática de peers

    N1->>N2: RequestVote
    N1->>N3: RequestVote
    N2-->>N1: vote granted
    N3-->>N1: vote granted
    Note over N1: node1 vence a eleição → Leader

    loop enquanto for líder
        N1->>M: GET /poll (long polling, N workers)
        M-->>N1: 200 OK { evento fake }
        N1->>N1: printa evento no stdout
    end

    Note over N2,N3: seguem como followers,<br/>recebendo heartbeats de node1

    rect rgb(255, 230, 230)
        Note over N1: docker compose stop node1
    end

    N2-xN1: heartbeat timeout
    N3-xN1: heartbeat timeout
    N2->>N3: RequestVote
    N3-->>N2: vote granted
    Note over N2: node2 vence a nova eleição → Leader

    loop enquanto for líder
        N2->>M: GET /poll
        M-->>N2: 200 OK { evento fake }
        N2->>N2: printa evento no stdout
    end
```

## Como rodar

```bash
docker compose up -d --build
```

Depois de alguns segundos, um dos nós vira líder e passa a imprimir eventos:

```bash
docker compose logs -f node1 node2 node3
```

Consultar o estado de qualquer nó:

```bash
curl localhost:8081/status   # node1
curl localhost:8082/status   # node2
curl localhost:8083/status   # node3
```

## Testando o failover

```bash
docker compose stop node2     # supondo que node2 seja o líder atual
docker compose logs -f node1 node3   # um deles assume e retoma o polling em segundos
docker compose start node2    # volta ao cluster como follower
```

## Configuração

| Variável (env) | Onde | Efeito |
|---|---|---|
| `POLL_WORKERS` | `app` | Nº de goroutines de long polling concorrentes quando o nó é líder (vazão de eventos) |
| `POLL_MIN_DELAY` / `POLL_MAX_DELAY` | `mock-server` | Intervalo aleatório até um novo evento "aparecer" |
| `POLL_MAX_WAIT` | `mock-server` | Timeout do long polling (retorna `204` se não houver evento) |

## Encerrar

```bash
docker compose down
```

Detalhes de requisitos, arquitetura e decisões técnicas: veja [SPEC.md](SPEC.md).
