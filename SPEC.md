# SPEC — Leader Election & Long Polling com Raft (POC)

## 1. Contexto e Objetivo

Prova de conceito para validar eleição de líder distribuída usando a implementação
do Raft da HashiCorp (`github.com/hashicorp/raft`) em Go, aplicada a um cenário de
**pooling coordenado**: entre N réplicas idênticas de uma aplicação, apenas a réplica
eleita **líder** deve realizar long polling contra uma API mockada e imprimir o
resultado. As réplicas não-líder permanecem ociosas (apenas participando do
protocolo de consenso), prontas para assumir a liderança caso o líder atual caia.

As réplicas devem se comportar como um `StatefulSet` do Kubernetes — identidade
estável (`node1`, `node2`, `node3`), conhecimento prévio dos peers — mas a POC roda
localmente via **docker-compose**, sem depender de Kubernetes.

Prioridade explícita: **simplicidade e legibilidade** do código Raft acima de
robustez de produção.

## 2. Requisitos Funcionais

- RF01 — Formar um cluster Raft de 3 nós (`node1`, `node2`, `node3`) que se
  conhecem via lista estática de peers (env var), com bootstrap automático na
  primeira subida.
- RF02 — Exatamente um nó deve ser líder a qualquer momento (garantia do próprio
  Raft).
- RF03 — Apenas o nó líder deve executar o loop de long polling contra o
  mock-server e imprimir o payload recebido no stdout.
- RF04 — Ao perder a liderança (ou perder o próprio papel de líder), o nó deve
  interromper imediatamente o loop de polling.
- RF05 — Ao derrubar o container do líder atual (`docker compose stop <node>`),
  os 2 nós restantes devem eleger um novo líder automaticamente, que assume o
  polling sem intervenção manual.
- RF06 — Cada nó expõe um endpoint HTTP `GET /status` retornando o estado atual
  do Raft (líder atual, papel do nó, membros conhecidos).
- RF07 — O mock-server expõe um endpoint de long polling que segura a conexão
  aberta até haver um "novo evento" simulado (delay aleatório) ou até um timeout,
  retornando um evento gerado com faker.

## 3. Requisitos Não-Funcionais

- RNF01 — Código o mais simples e legível possível; evitar abstrações
  desnecessárias (sem FSM de negócio real — apenas o mínimo exigido pela
  interface `raft.FSM`).
- RNF02 — Sem persistência: log store, stable store e snapshot store do Raft em
  memória (`raft.NewInmemStore`). Estado é perdido ao reiniciar o container —
  aceitável para a POC.
- RNF03 — Sem testes automatizados nesta fase; validação é manual/observacional
  via logs e endpoint `/status`.
- RNF04 — Reeleição deve ocorrer em segundos (usar timeouts padrão/curtos do
  Raft, compatíveis com ambiente local).

## 4. Arquitetura

### 4.1 Componentes

```
leader-election-pooling/
├── app/                  # aplicação Go que participa do cluster Raft
│   ├── go.mod
│   ├── main.go           # bootstrap, wiring
│   ├── raft.go           # setup do raft.Raft (transport, stores, FSM)
│   ├── fsm.go            # FSM mínima (no-op) exigida pela lib
│   ├── leader.go         # leader semaphore + loop de long polling
│   └── httpapi.go        # endpoint /status
├── mock-server/          # serviço HTTP mockado
│   ├── go.mod
│   └── main.go           # endpoint /poll com long polling + faker
├── docker-compose.yml
└── SPEC.md
```

### 4.2 Topologia (docker-compose)

- `mock-server` — serviço único, porta HTTP exposta (ex: `8090`).
- `node1`, `node2`, `node3` — 3 instâncias do mesmo binário `app`, cada uma com:
  - `RAFT_NODE_ID` (ex: `node1`), caso não informado, deverá pegar o hostname
  - `RAFT_BIND_ADDR` (ex: `node1:7000`) — porta Raft (transport TCP interno)
  - `RAFT_PEERS` (ex: `node1=node1:7000,node2=node2:7000,node3=node3:7000`)
  - `HTTP_ADDR` (ex: `:8080`) — porta do `/status`, mapeada para portas
    distintas no host (`8081`, `8082`, `8083`)
  - `MOCK_SERVER_URL` (ex: `http://mock-server:8090/poll`)

### 4.3 Fluxo de eleição e semáforo de líder

1. Cada nó sobe, inicializa `raft.NewRaft(...)` com transporte TCP próprio e
   stores em memória.
2. Um único nó (o de menor ID, por convenção, ou o primeiro a checar que o
   cluster ainda não existe) executa `BootstrapCluster` com a lista completa de
   peers vinda de `RAFT_PEERS`. Os demais apenas sobem seus próprios `raft.Raft`
   apontando para os mesmos peers — o Raft resolve a eleição entre eles.
3. Cada nó registra um `raft.Observer` (ou consome `raft.LeaderCh()`) para
   detectar mudanças de liderança.
4. O "leader semaphore" é uma goroutine controladora: ao receber sinal de que o
   nó virou líder, dá `start()` num loop de long polling (com `context.Context`
   cancelável); ao receber sinal de perda de liderança, cancela o contexto e
   para o loop imediatamente.
5. O loop de polling chama `GET /poll` no mock-server, e ao receber resposta
   imprime o JSON no stdout prefixado com o node ID, então repete a chamada.

### 4.4 Contrato do mock-server — `GET /poll`

- Segura a conexão (long polling real) até:
  - Um delay aleatório (ex: 2–5s) expirar → retorna `200 OK` com um evento
    fake, formato:
    ```json
    {
      "id": "uuid",
      "event_type": "string",
      "payload": { "...": "campos gerados por faker" },
      "timestamp": "RFC3339"
    }
    ```
  - Um timeout máximo (ex: 30s) expirar sem novo evento → retorna `204 No
    Content`, e o cliente deve reabrir a conexão imediatamente.

### 4.5 Contrato do app — `GET /status`

```json
{
  "node_id": "node1",
  "state": "Leader | Follower | Candidate",
  "leader_address": "node2:7000",
  "peers": ["node1:7000", "node2:7000", "node3:7000"]
}
```

## 5. Decisões Técnicas (resumo das escolhas)

| Decisão | Escolha | Justificativa |
|---|---|---|
| Storage Raft | In-memory (`raft.NewInmemStore`) | Prioriza simplicidade/legibilidade da POC; não há requisito de persistência entre restarts. |
| Descoberta de peers | Lista estática via env var + bootstrap automático | Reflete a analogia com StatefulSet (identidade e peers conhecidos de antemão), sem exigir mecanismo de discovery adicional. |
| Long polling do mock | Long polling real (conexão held) | Demonstra de forma mais fiel o padrão real que está sendo simulado, em vez de apenas um polling curto disfarçado. |
| Domínio dos dados mockados | Eventos genéricos (`event_type`, `payload`, `timestamp`) | Mantém a POC focada no mecanismo de coordenação, sem amarrar a um domínio de negócio específico. |
| Observabilidade | Endpoint `/status` por nó + logs no stdout | Permite verificar o estado do cluster via `curl` sem depender só de leitura de logs. |
| Simulação de falha | `docker compose stop/kill <node>` | Reflete fielmente falha de pod/réplica no Kubernetes (SIGTERM/SIGKILL). |
| Testes automatizados | Nenhum nesta fase | Escopo da POC é demonstração manual; testes ficam para uma fase futura se a POC evoluir. |

## 6. Plano de Execução (tasks)

1. **mock-server**: criar módulo Go, endpoint `GET /poll` com long polling +
   `gofakeit` (ou `faker`) gerando eventos genéricos; delay aleatório e timeout
   configuráveis via env var.
2. **app — FSM mínima**: implementar `raft.FSM` no-op (Apply/Snapshot/Restore
   sem lógica de negócio real, apenas satisfazendo a interface).
3. **app — setup Raft**: configurar `raft.Config`, `raft.NewTCPTransport`,
   stores em memória, parse de `RAFT_PEERS`, bootstrap condicional.
4. **app — leader semaphore**: goroutine que observa transições de liderança
   (`raft.LeaderCh()`/Observer) e start/stop do loop de polling via contexto
   cancelável.
5. **app — loop de polling**: cliente HTTP que consome `GET /poll` do
   mock-server em loop contínuo, e imprime o payload recebido.
6. **app — HTTP status API**: endpoint `/status` expondo estado do nó/cluster.
7. **docker-compose**: definir os 4 serviços (`mock-server`, `node1`, `node2`,
   `node3`), rede interna comum, variáveis de ambiente, portas mapeadas.
8. **Validação manual** (ver critérios de aceite abaixo).

## 7. Critérios de Aceite / Roteiro de Validação Manual

1. `docker compose up` — os 3 nós sobem, formam cluster, um deles se torna
   líder (visível via logs e `curl localhost:8081/status` etc.).
2. Apenas o nó líder imprime eventos recebidos do mock-server; os outros dois
   ficam silenciosos quanto ao polling.
3. `docker compose stop <node-líder-atual>` — em poucos segundos, um dos nós
   restantes assume a liderança e passa a imprimir os eventos do polling.
4. `curl localhost:<porta>/status` em cada nó restante confirma o novo líder
   de forma consistente entre eles.
5. Subir novamente o nó parado — ele deve se reintegrar ao cluster como
   follower, sem interromper o líder atual.

## 8. Extensão — Persistência de Eventos com bbolt (WAL)

### 8.1 Objetivo

Persistir em disco, por réplica, cada evento recebido do mock-server enquanto
o nó é líder, usando [bbolt](https://github.com/etcd-io/bbolt) como um
write-ahead log (WAL) append-only. Garante que o histórico de eventos
processados por uma réplica sobrevive a restarts do container, sem alterar o
comportamento do cluster Raft em si.

### 8.2 Requisitos Funcionais (novos)

- RF08 — Sempre que o nó líder receber um evento com sucesso (`200 OK`) do
  `/poll`, ele deve gravar esse evento em um bucket bbolt local antes de
  imprimi-lo no stdout.
- RF09 — A gravação deve ser durável: a transação bbolt só é considerada
  concluída após o commit ir a disco (fsync).
- RF10 — Cada réplica (`node1`, `node2`, `node3`) tem seu próprio arquivo
  `.db` bbolt, persistido em um volume Docker nomeado individual, sobrevivendo
  a `docker compose stop/start` daquele nó.
- RF11 — O WAL é estritamente local: não há replicação do conteúdo do bbolt
  entre nós via Raft. Um nó que nunca foi líder terá seu bbolt vazio (ou só
  com os eventos do período em que foi líder).
- RF12 — Cada nó expõe `GET /events`, listando todo o conteúdo do seu bucket
  `events` em JSON, ordenado pelo campo `timestamp` de cada evento (não pela
  chave do bucket, que é o uuid).

### 8.3 Requisitos Não-Funcionais (novos)

- RNF05 — Não altera a FSM do Raft (RNF01) nem os stores do Raft (RNF02) — a
  persistência do WAL é inteiramente desacoplada do protocolo de consenso.
- RNF06 — A escrita no bbolt não deve travar o loop de polling além do
  necessário: cada evento abre e fecha sua própria transação `Update` (bbolt é
  single-writer por design, então múltiplos poll workers gravando ao mesmo
  tempo são automaticamente serializados pelo próprio bbolt, sem lock extra no
  código da aplicação).

### 8.4 Decisões Técnicas (resumo)

| Decisão | Escolha | Justificativa |
|---|---|---|
| Escopo do WAL | Local por réplica — só quem é líder no momento grava, no seu próprio volume | Mantém a FSM no-op e o RNF02 intactos; sem round-trip de consenso por evento, não afeta a vazão do polling. |
| Chave do bucket | `event.id` (uuid gerado pelo mock-server) | Permite lookup direto por id. Como consequência, a iteração natural do bucket (ordenada por bytes da chave) **não reflete a ordem cronológica de chegada** — quem precisar da ordem de chegada deve ordenar pelo campo `timestamp` já presente no valor, não confiar na ordem das chaves. |
| Volumes | 1 volume Docker nomeado por nó (`wal-node1`, `wal-node2`, `wal-node3`) montado em `/data` | bbolt não suporta múltiplos processos escrevendo no mesmo arquivo `.db` simultaneamente; replica a identidade estável de um StatefulSet, onde cada réplica mantém seu próprio estado em disco. |
| Durabilidade | Fsync por transação (`bbolt.Options` padrão, `NoSync: false`) | É o que dá sentido a chamar isso de WAL — um evento só é considerado salvo depois do commit ir a disco. |

### 8.5 Design

- Um único arquivo bbolt por nó: `/data/wal.db` (path configurável via env
  `WAL_PATH`, default `/data/wal.db`).
- Um único bucket: `events`.
- Chave = `[]byte(event.ID)`; valor = o JSON bruto do evento, exatamente como
  recebido do `/poll`.
- `bbolt.DB` é aberto uma única vez no startup do processo (`main.go`) e
  reaproveitado por todos os poll workers durante o período em que o nó é
  líder.
- Cada evento recebido em `handlePollResponse` é persistido numa transação
  `db.Update(...)` própria, antes do `log.Printf` de impressão; erro de
  gravação é logado mas não derruba o loop de polling.
- `GET /events` (RF12) faz um único `db.View(...)`, itera o bucket inteiro
  com `ForEach`, e ordena o resultado em memória por `timestamp` antes de
  responder — aceitável para o volume de dados de uma POC; não pagina.

### 8.6 Estrutura de diretórios (novos arquivos)

```
app/
├── ...
└── wal.go          # abertura do bbolt.DB + função SaveEvent(event)
```

### 8.7 Contrato interno — `wal.go`

```go
func OpenWAL(path string) (*bbolt.DB, error)
func SaveEvent(db *bbolt.DB, event map[string]any) error
```

- `OpenWAL` garante que o diretório de `path` existe, abre o `bbolt.DB` e cria
  o bucket `events` se não existir (`db.Update` com
  `CreateBucketIfNotExists`).
- `SaveEvent` extrai `event["id"]` como string, faz `json.Marshal(event)` e
  grava `bucket.Put([]byte(id), jsonBytes)` dentro de uma transação
  `db.Update`.

### 8.8 docker-compose — volumes

```yaml
services:
  node1:
    ...
    environment:
      WAL_PATH: "/data/wal.db"
    volumes:
      - wal-node1:/data
  node2:
    ...
    environment:
      WAL_PATH: "/data/wal.db"
    volumes:
      - wal-node2:/data
  node3:
    ...
    environment:
      WAL_PATH: "/data/wal.db"
    volumes:
      - wal-node3:/data

volumes:
  wal-node1:
  wal-node2:
  wal-node3:
```

### 8.9 Plano de Execução (tasks)

1. Adicionar dependência `go.etcd.io/bbolt` ao módulo `app`.
2. Criar `app/wal.go` com `OpenWAL` e `SaveEvent`.
3. `main.go`: ler `WAL_PATH` (default `/data/wal.db`), abrir o WAL no
   startup, `defer db.Close()`, e passar o `*bbolt.DB` adiante para
   `leaderSemaphore`/`startPolling`/`pollLoop`.
4. `leader.go` / `handlePollResponse`: após decodificar o evento com sucesso
   (`200 OK`), chamar `SaveEvent` antes do `log.Printf`.
5. `docker-compose.yml`: declarar os 3 volumes nomeados, montá-los em `/data`
   nos respectivos serviços `node1`/`node2`/`node3`, e setar `WAL_PATH`.
6. `README.md`: documentar a nova variável `WAL_PATH` e o comportamento do
   WAL (seção Configuração + uma nota no Fluxo/Arquitetura).

### 8.10 Critérios de Aceite

1. `docker compose up` — o líder grava cada evento recebido no seu `wal.db`
   local.
2. Inspecionar o arquivo do líder (ex.: `docker compose exec <líder> sh` e
   usar a CLI `bbolt` ou um script Go de leitura) mostra uma entrada por
   evento impresso no log, com a chave batendo com o `id` do evento.
3. `docker compose stop <líder>` seguido de `docker compose start <líder>` —
   o `wal.db` daquele nó permanece intacto (conteúdo preservado no volume),
   mesmo o nó tendo perdido a liderança nesse meio tempo.
4. `docker compose down` (sem `-v`) seguido de `docker compose up` novamente
   — os volumes nomeados preservam o conteúdo do bbolt entre subidas
   completas do cluster.
5. Um nó que nunca assumiu liderança tem `wal.db` vazio (só o bucket
   `events` criado, sem entradas).
