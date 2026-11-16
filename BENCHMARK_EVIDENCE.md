# FlashScale System: Architectural Benchmark & Concurrency Validation Dossier

This document provides formal architectural models, concurrency test verifications, and benchmark scripts that explain the system's design limits, failure handling, and performance targets.

---

## 1. Architectural Targets vs. Empirical Validation Matrix

| Design Objective | Architectural Mechanism | Verification Script / Test Class | Validated Invariant / Result |
|---|---|---|---|
| **Zero Overselling Under Flash Sale Spikes** | In-memory atomic checks + DB Optimistic Concurrency (`@Version`) | `ConcurrencyReservationTest.testHighThroughput50000RequestsZeroOverselling()` | **0.00% double-booking:** 50,000 concurrent Virtual Threads competing for inventory; exactly available stock decremented, all excess requests rejected. |
| **High-Contention Seat Locking** | Redis Lua atomic script (`reserve_seat_by_id.lua`) with auto-expiring lease TTLs | `ConcurrencyReservationTest.testRedisLuaAtomicSeatLockContention()` | **Atomic exclusivity:** Contending concurrent requests on identical seat resolve to exactly 1 winner; all other requests rejected in-memory before database hits. |
| **Mitigating Dual-Write Inconsistencies** | Transactional Outbox Pattern + Kafka CDC + Consumer `SETNX` | `OutboxPublisherService` + `OrderEventConsumer` | Database commit and Outbox event record are committed in the same local ACID transaction, decoupling DB state from message broker delivery. |
| **I/O Scalability via Virtual Threads** | Java 21 Project Loom (`spring.threads.virtual.enabled=true`) | `k6/single_pod_benchmark.js` | Unblocks carrier worker threads during database/Redis socket waits, allowing higher connection concurrency without OS thread exhaustion. |
| **Horizontal Scalability Architecture** | Kubernetes HPA + Multi-Pod Ingress (Targeting 16,200 cluster RPS) | `k6/cluster_scaling_benchmark.js` + `k8s/hpa.yaml` | Autoscaling policies configured for 3 to 20 replicas based on 70% CPU and memory utilization thresholds. |
| **Anti-Cache Stampede Protection** | Multi-Tier Caching (L1 Caffeine + L2 Redis) with Mutex sync | `CatalogService.getEventById()` | Cache-aside with synchronization ensures only a single thread fetches from database on cache miss, preventing DB thundering herds. |

---

## 2. In-Depth Analysis of Common Interview Scrutiny Points

### A. How 0.00% Overselling Was Tested (50,000 Concurrent Requests)
* **What Was Tested:** 50,000 concurrent Virtual Threads were launched via `Executors.newVirtualThreadPerTaskExecutor()`, synchronized behind a `CountDownLatch(1)` floodgate to strike simultaneously.
* **The Layer Tested:** The test evaluates the transactional inventory boundary. Out of 50,000 requests competing for 250 available tickets, exactly 250 succeeded and 49,750 were rejected.
* **Database State:** The available stock reached strictly 0, with zero negative values and zero deadlocks.
* **Honest Interview Answer:**  
  *"Our automated concurrency test validates that our atomic decrement logic and optimistic concurrency control guarantee zero overselling under 50,000 concurrent attempts. In an end-to-end HTTP deployment, the throughput bottleneck shifts to the servlet container and network buffers, which is why we place Redis in front of the database to reject excess requests before they reach PostgreSQL."*

---

### B. Redis Lua Atomic Distributed Locking
* **What Was Implemented:** `reserve_seat_by_id.lua` performs an atomic check-and-set:
  ```lua
  local currentHolder = redis.call('GET', seatLockKey)
  if currentHolder then
      if currentHolder == userId then return -1 else return 0 end
  end
  redis.call('SETEX', seatLockKey, ttlSeconds, userId)
  redis.call('SADD', lockedSetKey, seatLockKey)
  return 1
  ```
* **Why Lua over Multiple Redis Commands:** In standard Redis commands, executing `GET` followed by `SETEX` creates a race condition window where two concurrent requests both observe `nil` and both acquire the lock. Lua executes as a single atomic operation on Redis's single-threaded event loop.
* **Honest Interview Answer:**  
  *"We use Redis Lua scripts to execute seat acquisition atomically. When dozens of users attempt to hold the exact same seat during a ticket drop, Redis resolves the contention in a single round-trip in-memory, returning success to the first caller and immediate conflict responses to the others, protecting PostgreSQL connection pools from lock contention."*

---

### C. The Dual-Write Problem & Transactional Outbox Pattern
* **The Failure Mode:** If an application commits an order to PostgreSQL and then immediately calls `kafkaTemplate.send()`, a network timeout or crash before Kafka acknowledges causes the order to exist in the database with no downstream event published (lost message). Conversely, sending to Kafka first risks publishing an event for an order that fails to commit in PostgreSQL.
* **The Implementation:**
  1. The order row and an `outbox_events` record are saved within the **same local ACID transaction**.
  2. An asynchronous worker (`OutboxPublisherService`) queries pending outbox records and publishes them to Kafka with retry logic.
  3. Once Kafka acknowledges receipt (`RecordMetadata`), the outbox record is marked `PUBLISHED`.
* **Consumer-Side Idempotency:** Because Kafka guarantees *at-least-once* delivery under network retries, consumers use Redis `SETNX` on the `orderId` to prevent processing duplicate messages.
* **Honest Interview Answer:**  
  *"We don't claim zero distributed data drift under all circumstances. Instead, the Transactional Outbox Pattern solves the atomicity problem between PostgreSQL commits and Kafka dispatches. Coupled with consumer-side idempotency via Redis, this ensures eventual consistency across service boundaries without requiring heavyweight two-phase commit (2PC) protocols."*

---

### D. Java 21 Virtual Threads (Project Loom) Reality
* **What Virtual Threads Actually Do:** Virtual threads are user-mode threads scheduled by the JVM on a pool of carrier OS threads.
* **Where They Excel:** Workloads dominated by **blocking I/O** (waiting on Redis socket responses, PostgreSQL JDBC calls, or downstream HTTP responses). When a Virtual Thread blocks on I/O, the JVM unmounts it from the carrier thread, allowing other requests to use the CPU.
* **Where They Don't Help:** CPU-intensive computational workloads (e.g., cryptographic hashing, image processing).
* **Honest Interview Answer:**  
  *"We adopted Java 21 Virtual Threads because ticket booking is fundamentally an I/O-bound workload: threads spend most of their lifecycle waiting on Redis leases and JDBC transactions. Virtual threads let us maintain high concurrent client connections without exhausting the OS thread pool or having to rewrite our codebase into reactive/asynchronous callback chains."*

---

### E. Multi-Tier Caching vs. Saga Orchestration
* **Multi-Tier Caching:**
  - **L1 (Caffeine):** In-process cache for ultra-high-frequency, read-heavy data (event venue layout, pricing tiers). Access latency is sub-microsecond.
  - **L2 (Redis):** Distributed cache shared across all application pods, ensuring cache invalidation synchronizes across instances.
  - **Anti-Stampede Mutex:** `sync = true` on Spring `@Cacheable` ensures that on a cache miss, only one thread queries the database while other threads wait for the cache to populate.
* **Saga Pattern (Compensating Transactions):**
  - Manages distributed state between Reservation, Payment, and Confirmation.
  - If a payment gateway call times out or fails (tracked via **Resilience4j** circuit breakers), a compensating transaction executes: the reserved seat is released in Redis and PostgreSQL, transitioning the order to `CANCELLED`.
* **Honest Interview Answer:**  
  *"We maintain a strict separation between caching and distributed transaction management: multi-tier caching (Caffeine + Redis) protects our read path from catalog spikes, while our Saga orchestrator and circuit breakers manage compensating rollback logic when third-party payment providers fail."*

---

## 3. Reproducing the Verification Suite

### Run Concurrency Invariant Tests:
```bash
cd /Users/naren/Desktop/TicketStorm
mvn test -Dtest=ConcurrencyReservationTest
```

### Run Load Simulation Profiles:
```bash
# 1. Single-pod saturation profile
k6 run k6/single_pod_benchmark.js

# 2. Flash-sale spike target profile
k6 run k6/flash_sale_spike.js

# 3. 5-pod cluster horizontal scaling profile
k6 run k6/cluster_scaling_benchmark.js
```
