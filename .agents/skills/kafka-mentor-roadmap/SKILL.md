---
name: kafka-mentor-roadmap
description: >-
  Quản lý và điều phối lộ trình học Apache Kafka thực chiến với Java từ cơ bản đến chuyên sâu (Production-grade).
  Sử dụng skill này khi học viên bắt đầu bài học mới, yêu cầu kiểm tra tiến độ, hoặc chuyển giao giữa các module.
---

# Lộ Trình Học Kafka Thực Chiến & Chuyên Sâu (Java Developer Roadmap)

Skill này cung cấp khung chương trình chi tiết 8 Module chuẩn kỹ sư hệ thống phân tán, kèm mục tiêu và bài tập kiểm tra (Checkpoints).

---

## Danh Sách Các Module Học Tập

### Module 1: Bản Chất Kiến Trúc Phân Tán Của Kafka (Under the Hood)
- **Mục tiêu**: Hiểu tại sao Kafka được sinh ra, sự khác biệt giữa Kafka và Traditional MQ (RabbitMQ, ActiveMQ).
- **Trọng tâm**:
  - Cơ chế Append-Only Commit Log và Sequential I/O (Lý do Kafka nhanh gấp bội Disk Random I/O).
  - Page Cache & Zero-Copy (Linux `sendfile` system call).
  - Khái niệm: Broker, Cluster, Topic, Partition, Offset, Segment (`.log`, `.index`, `.timeindex`).
  - Phân tán dữ liệu: Leader Partition vs Follower Partition, In-Sync Replicas (ISR), High Watermark (HW), Log End Offset (LEO).
- **Thực hành**:
  - Dựng cụm Kafka KRaft mode (Kafka hiện đại không cần ZooKeeper) bằng Docker Compose.
  - Sử dụng Kafka UI để xem cấu trúc Topic, Partition và Segment.

---

### Module 2: Producer Chuyên Sâu & Cấu Hình Đảm Bảo Dữ Liệu
- **Mục tiêu**: Làm chủ `KafkaProducer` trong Java, nắm vững luồng gửi message từ RecordAccumulator đến Sender thread.
- **Trọng tâm**:
  - Luồng nội bộ: `ProducerRecord` -> Serializer -> Partitioner -> Accumulator Buffer -> Network Client.
  - Các chiến lược Partitioning: Default Partitioner (Murmur2 hash), Round-Robin, Sticky Partitioner, Custom Partitioner.
  - Bộ 3 config quyết định độ tin cậy: `acks` (0, 1, all / -1), `min.insync.replicas`, `retries`.
  - Idempotent Producer: Cơ chế chống duplicate bằng `ProducerId` (PID) và `Sequence Number`.
  - Tối ưu Throughput: `batch.size`, `linger.ms`, `compression.type` (snappy, lz4, zstd).
- **Thực hành**:
  - Viết Java Producer hoàn chỉnh: Gửi message bất đồng bộ (`Future` & Callback), xử lý Exception, Shutdown Hook.
  - Tự viết Custom Partitioner phân bổ dữ liệu theo User Region hoặc VIP Customer.

---

### Module 3: Consumer & Consumer Group Chuyên Sâu
- **Mục tiêu**: Hiểu tường tận chu trình Poll Loop, quản lý Offset và cơ chế Rebalance.
- **Trọng tâm**:
  - Poll loop: Single-threaded event loop, `max.poll.records`, `max.poll.interval.ms`, `heartbeat.interval.ms`, `session.timeout.ms`.
  - Quản lý Offset: Auto Commit vs Manual Commit (`commitSync` vs `commitAsync`), xử lý At-Least-Once vs At-Most-Once.
  - Consumer Rebalance: Khi nào xảy ra? Eager Rebalance vs Cooperative Sticky Rebalance (Incremental Rebalance).
  - Rebalance Listener: Lưu offset vào database ngoại vi để đạt Exactly-Once trên Consumer.
  - Multithreading Patterns: 1 Consumer per Thread vs Decoupled Consumer & Worker Thread Pool.
- **Thực hành**:
  - Viết Java Consumer với Manual Commit chuẩn và Graceful Shutdown (`WakeupException`).
  - Viết Worker Pool Consumer xử lý message bất đồng bộ, bảo đảm trật tự theo Key.

---

### Module 4: Khả Năng Chịu Lỗi & Độ Tin Cậy (High Availability & Fault Tolerance)
- **Mục tiêu**: Xây dựng hệ thống không bao giờ mất dữ liệu (Zero Data Loss Architecture).
- **Trọng tâm**:
  - `replication.factor`, Leader election, Unclean leader election (`unclean.leader.election.enable=false`).
  - Tình huống mất dữ liệu và cách phòng tránh.
- **Thực hành**:
  - Giả lập tắt đột ngột Broker (Kill container) và kiểm tra quá trình Failover, quan sát ISR thay đổi.

---

### Module 5: Giao Dịch Trong Kafka (Kafka Transactions & Exactly-Once Semantics - EOS)
- **Mục tiêu**: Nắm vững mô hình Read-Process-Write với Exactly-Once Semantics (EOS).
- **Trọng tâm**:
  - Transaction Coordinator & Transaction Log (`__transaction_state`).
  - 2-Phase Commit (2PC) trong Kafka.
  - Cấu hình: `transactional.id`, `isolation.level=read_committed`.
- **Thực hành**:
  - Viết chương trình Java nhận đơn hàng từ Topic A, xử lý thanh toán, ghi sang Topic B và commit offset cùng trong một Transaction.

---

### Module 6: Xử Lý Lỗi, Retry Non-Blocking & Dead Letter Queue (DLQ)
- **Mục tiêu**: Xử lý tin nhắn lỗi (Poison Pill) mà không làm tắc nghẽn luồng dữ liệu (Head-of-Line Blocking).
- **Trọng tâm**:
  - Retry Topic Pattern (Topic-order-retry-1, Topic-order-retry-2, Topic-order-dlq).
  - Exponential Backoff.
- **Thực hành**:
  - Xây dựng hệ thống xử lý lỗi với Dead Letter Topic và cơ chế Reprocess/Alert.

---

### Module 7: Tích Hợp Spring Boot & Spring Kafka
- **Mục tiêu**: Áp dụng kiến thức cốt lõi vào framework phổ biến nhất trong doanh nghiệp.
- **Trọng tâm**:
  - `@KafkaListener`, `KafkaTemplate`.
  - `DefaultErrorHandler`, `DeadLetterPublishingRecoverer`.
  - Concurrency, Batch listener, Filter, Serialization with Jackson Json.
- **Thực hành**:
  - Xây dựng Microservice Order & Notification hoàn chỉnh với Spring Boot 3 + Spring Kafka.

---

### Module 8: Tối Ưu Hóa & Giám Sát Production (Performance Tuning & Monitoring)
- **Mục tiêu**: Tối ưu hiệu năng, phát hiện sự cố trước khi hệ thống down.
- **Trọng tâm**:
  - Đo lường Consumer Lag, Offset Lag.
  - Log Compaction & Tombstone message.
  - Các metrics quan trọng: UnderReplicatedPartitions, IsrShrinks/IsrExpands, NetworkProcessorAvgIdlePercent.
- **Thực hành**:
  - Thiết lập topic compaction, gửi message update và tombstone, kiểm chứng quá trình dọn dẹp segment.
