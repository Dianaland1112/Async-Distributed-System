# 비동기 이벤트 플랫폼 — 데이터 흐름 아키텍처

## 1. 전체 아키텍처 흐름

```mermaid
flowchart TD
    Client(["👤 Client"])
    ALB["🔀 ALB\n(Application Load Balancer)"]
    Ingest["📥 Ingest API\n:8080\nParticipationController\nParticipationService"]
    SQS[["📨 SQS\nMain Queue"]]
    DLQ[["☠️ SQS\nDead Letter Queue"]]
    Worker["⚙️ Worker\n:8081\nSqsMessageConsumer\n(ThreadPool x4)"]
    EB["📡 EventBridge\nParticipationProcessedEvent"]

    DDB_STATE[("🗄️ DynamoDB\nrequest-state\nPK: requestId")]
    DDB_IDEMP[("🗄️ DynamoDB\nworker-idempotency\nPK: IDEMP#eventId#userId")]
    DDB_CAP[("🗄️ DynamoDB\nevent-capacity\nPK: eventId")]

    Client -->|"POST /api/events/{eventId}/apply\n?eventType=FIRST_COME\nX-User-Id: {userId}"| ALB
    ALB --> Ingest
    Ingest -->|"202 Accepted\n{ requestId }"| Client
    Ingest -->|"sendMessage\nParticipationMessage\n(requestId, eventId, userId,\nqueuedAt, eventType)"| SQS

    SQS -->|"pollMessages\nbatch=5, waitSeconds=5\ndelay=3000ms"| Worker

    Worker -->|"tryLock\nIDEMP#eventId#userId"| DDB_IDEMP
    Worker -->|"createReceived → markQueued\n→ tryAcquireProcessing (CAS)\n→ markSucceeded / markRejected\n/ markFailedFinal"| DDB_STATE
    Worker -->|"tryDecrement(eventId)\nFIRST_COME 한정"| DDB_CAP
    Worker -->|"publish\nParticipationProcessedEvent"| EB
    Worker -->|"NON_RETRYABLE\n즉시 DLQ + ACK"| DLQ
```

---

## 2. Worker 메시지 처리 상세 흐름

```mermaid
flowchart TD
    POLL["📥 pollMessages()\n@Scheduled fixedDelay=3s\nSQS long-poll batch=5"]
    PARSE["파싱\nobjectMapper.readValue(body)"]
    PARSE_FAIL["body 파싱 실패\n→ messageAttributes 복구 시도"]
    INVALID["복구 불가\nINVALID_SCHEMA\n→ DLQ + ACK"]
    IDEMP["idempotencyRepository.tryLock\n(eventId, userId, requestId)\nDDB CAS: attribute_not_exists\nOR requestId = :rid"]
    DUP["중복 요청\n→ ACK (skip)"]
    CREATE["requestStateRepository\n.createReceived(requestId, ...)"]
    QUEUED["requestStateRepository\n.markQueued(requestId, queuedAt)"]
    ACQUIRE["requestStateRepository\n.tryAcquireProcessing(requestId, startedAt)\nCAS: status=QUEUED → PROCESSING"]

    ACQ_FAIL["선점 실패\n→ getCurrentStatus()"]
    RECEIVED_ST["status=RECEIVED\nattempt ≤ 3: deferMessage(2s)\nattempt > 3: STALE_RECEIVED\n→ markFailedFinal + DLQ + ACK"]
    PROC_ST["status=PROCESSING\nattempt ≤ 5: deferMessage(2s)\nattempt > 5: STALE_PROCESSING\n→ markFailedFinal + DLQ + ACK"]
    OTHER_ST["status=SUCCEEDED/REJECTED\n→ ACK (already done)"]

    BIZ["비즈니스 로직\neventType == FIRST_COME?"]
    CAP_OK["eventCapacityRepository\n.tryDecrement(eventId) → true\n→ markSucceeded"]
    CAP_NO["eventCapacityRepository\n.tryDecrement(eventId) → false\n→ markRejectedCapacity"]
    LOTTERY["LOTTERY / 기타\n→ markSucceeded"]
    EB["publisher.publish\n(ParticipationProcessedEvent)"]
    ACK["✅ deleteMessage (ACK)"]

    EX["예외 발생\nclassifyFailure(e)"]
    NON_RET["NON_RETRYABLE\n(IllegalArgumentException\nBusinessRuleViolationException\n4xx)"]
    RET["RETRYABLE\n(IOException, Timeout\nDynamoDB throttling, 5xx)"]
    FAIL_FINAL["markFailedFinal\n→ DLQ + ACK"]
    ROLLBACK["releaseProcessingToQueued\n(requestId, startedAt)\nCAS: status=PROCESSING\nAND startedAt=:startedAt"]
    DEFER["deferMessage\n지수 백오프\n2s→4s→8s→...→60s\nattempt ≤ 5: defer\nattempt > 5: SQS redrive에 맡김"]

    POLL --> PARSE
    PARSE -->|성공| IDEMP
    PARSE -->|실패| PARSE_FAIL
    PARSE_FAIL -->|복구 성공| IDEMP
    PARSE_FAIL -->|복구 실패| INVALID

    IDEMP -->|중복| DUP
    IDEMP -->|최초| CREATE
    CREATE --> QUEUED
    QUEUED --> ACQUIRE
    ACQUIRE -->|선점 성공| BIZ
    ACQUIRE -->|선점 실패| ACQ_FAIL
    ACQ_FAIL --> RECEIVED_ST
    ACQ_FAIL --> PROC_ST
    ACQ_FAIL --> OTHER_ST

    BIZ -->|FIRST_COME| CAP_OK
    BIZ -->|FIRST_COME| CAP_NO
    BIZ -->|기타| LOTTERY
    CAP_OK --> EB
    CAP_NO --> EB
    LOTTERY --> EB
    EB --> ACK

    ACQUIRE -->|예외| EX
    BIZ -->|예외| EX
    EX --> NON_RET
    EX --> RET
    NON_RET --> FAIL_FINAL
    RET --> ROLLBACK
    ROLLBACK --> DEFER
```

---

## 3. 상태 전이 다이어그램

```mermaid
stateDiagram-v2
    [*] --> RECEIVED : createReceived
    RECEIVED --> QUEUED : markQueued
    RECEIVED --> FAILED_FINAL : STALE_RECEIVED attempt gt 3
    QUEUED --> PROCESSING : tryAcquireProcessing CAS
    PROCESSING --> QUEUED : releaseProcessingToQueued rollback on RETRYABLE
    PROCESSING --> SUCCEEDED : markSucceeded
    PROCESSING --> REJECTED : markRejectedCapacity
    PROCESSING --> FAILED_FINAL : markFailedFinal NON_RETRYABLE or STALE_PROCESSING
    SUCCEEDED --> [*]
    REJECTED --> [*]
    FAILED_FINAL --> [*]
```

---

## 4. DynamoDB 테이블 설계

```mermaid
erDiagram
    REQUEST_STATE {
        string requestId PK
        string eventId
        string userId
        string status
        string eventType
        string uiResult
        string resultCode
        string failureClass
        string errorCode
        string errorMessage
        number requestedAt
        number queuedAt
        number startedAt
        number finishedAt
        number lastRetryAt
        number ttl
    }

    WORKER_IDEMPOTENCY {
        string idempotencyKey PK
        string requestId
        number ttl
    }

    EVENT_CAPACITY {
        string eventId PK
        number remainingSlots
    }
```

---

## 5. 실패 분류 및 DLQ 전략

```mermaid
flowchart LR
    EX["예외 발생"]

    NON["NON_RETRYABLE\n─────────────\nIllegalArgumentException\nBusinessRuleViolationException\nHTTP 4xx (SQS/DDB)"]
    RET["RETRYABLE\n─────────────\nIOException / Timeout\nConnectException\nDDB Throttling (429)\nDDB 5xx / SQS 5xx\nSdkClientException"]

    DLQ_SEND["DLQ 전송\n(failureType, reasonCode\nstackTraceDigest, workerId\nreceiveCount)"]
    ACK["ACK\n(deleteMessage)"]
    ROLLBACK["PROCESSING → QUEUED\n롤백 (CAS)"]
    BACKOFF["deferMessage\n지수 백오프\n(2s→4s→...→60s)"]
    REDRIVE["attempt > 5\nSQS redrive\n(maxReceiveCount)\n→ DLQ 자동 이동"]

    EX --> NON
    EX --> RET

    NON --> DLQ_SEND
    DLQ_SEND --> ACK

    RET --> ROLLBACK
    ROLLBACK --> BACKOFF
    BACKOFF -->|"attempt ≤ 5"| BACKOFF
    BACKOFF -->|"attempt > 5"| REDRIVE
```

---

## 핵심 설계 포인트 요약

| 항목 | 내용 |
|------|------|
| **Ingest 역할** | SQS sendMessage 1회만 → 항상 202 즉시 반환 (수용 계층) |
| **Worker 역할** | 상태 CAS, 비즈니스 로직, 멱등성, DLQ 전략 (처리 계층) |
| **멱등성 보장** | `IDEMP#eventId#userId` 키로 DDB CAS (`attribute_not_exists OR requestId=:rid`) |
| **중복 재전달 대응** | SQS는 at-least-once → idempotency lock으로 최초 1회만 처리 |
| **PROCESSING 고착 방지** | RETRYABLE 실패 시 `releaseProcessingToQueued(requestId, startedAt)` CAS 롤백 |
| **Stale 상태 감지** | RECEIVED > 3회, PROCESSING > 5회 → FAILED_FINAL + DLQ |
| **지수 백오프** | `2 << (attempt-1)` 초, 최대 60초 cap |
| **메트릭** | `duplicate_skip_count`, `non_retryable_dlq_count{reasonCode}`, `retryable_exception_count` |
