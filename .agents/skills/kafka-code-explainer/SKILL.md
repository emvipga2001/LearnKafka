---
name: kafka-code-explainer
description: >-
  Cung cấp tiêu chuẩn giải thích và mổ xẻ mã nguồn Java Kafka ở mức độ chuyên sâu (Under-the-Hood, Concurrency, Configuration Trade-offs, Memory Model).
  Sử dụng skill này khi học viên cần phân tích sâu một đoạn code, tìm hiểu cơ chế hoạt động bên trong của Producer/Consumer, hoặc review code.
---

# Java Kafka Deep-Dive Code Explainer

Skill này hướng dẫn phương pháp giải thích code Java Kafka theo chiều sâu, giúp học viên không chỉ nhớ cú pháp mà hiểu thấu cơ chế vật lý bên dưới.

---

## 1. Khung Phân Tích Code Chuẩn 5 Bước

Khi phân tích bất kỳ đoạn code Producer/Consumer nào, người thầy cần đi qua 5 khía cạnh:

### Bước 1: Mục đích & Bối cảnh kinh doanh (Why & Context)
- Đoạn code này giải quyết bài toán gì trong thực tế? (Ví dụ: Thanh toán đơn hàng đòi hỏi không mất dữ liệu, hay Log clickstream chấp nhận mất 1-2 tin nhắn để lấy throughput cực đại?)

### Bước 2: Cấu trúc đa luồng & Bộ nhớ (Threading & Memory Anatomy)
- **Đối với Producer**:
  - Main Thread: gọi `send()` -> đẩy message qua `Serializer` -> `Partitioner` -> ghi vào `RecordAccumulator` (BufferPool chia thành các `ProducerBatch` cỡ 16KB).
  - Background Sender Thread (KafkaThread): gom các batch đủ điều kiện (`batch.size` hoặc hết `linger.ms`), đóng gói thành `ClientRequest` và bắn qua socket NIO tới Broker.
- **Đối với Consumer**:
  - Poll Thread: Consumer KHÔNG an toàn luồng (Not Thread-Safe). Duy nhất một luồng được gọi `poll()`, `commitSync()`, `close()`.
  - Heartbeat Thread: Chạy ngầm trong nền gửi heartbeat định kỳ về Group Coordinator để chứng minh consumer còn sống.

### Bước 3: Phân tích Đánh đổi Cấu hình (Configuration Trade-Offs Matrix)
Luôn so sánh các lựa chọn cấu hình:
| Config | Giá trị | Ưu điểm | Nhược điểm / Rủi ro | Kịch bản sử dụng |
| :--- | :--- | :--- | :--- | :--- |
| `acks` | `0` | Latency thấp nhất | Mất dữ liệu nếu Broker sập | Metrics, Sensor IOT |
| `acks` | `1` | Tốc độ vừa phải, Leader đã nhận | Mất dữ liệu nếu Leader chết trước khi sync sang Follower | Hệ thống log thông thường |
| `acks` | `all` | Bền vững tuyệt đối (kết hợp `min.insync.replicas=2`) | Latency cao hơn, phụ thuộc mạng giữa các broker | Giao dịch tài chính, đơn hàng |

### Bước 4: Xử lý Lỗi & Khả năng phục hồi (Error Handling & Edge Cases)
- Các loại Exception trong Kafka:
  - **RetriableException** (`TimeoutException`, `NetworkException`, `NotLeaderOrFollowerException`): Client có thể tự thử lại an toàn.
  - **Non-RetriableException** (`RecordTooLargeException`, `SerializationException`, `TopicAuthorizationException`): Retry vô ích, phải ném ra hoặc gửi vào DLQ.
- Thảo luận trường hợp rủi ro: Broker bị treo mạng, OOM, Rebalance storm, Zombie consumer.

### Bước 5: Tiêu chuẩn Clean Code & Production Checklist
- Sử dụng Try-with-resources hoặc Graceful Shutdown (`Runtime.getRuntime().addShutdownHook(...)`).
- Sử dụng SLF4J Logger thay vì `System.out.println`.
- Đo lường thời gian xử lý và bắt metrics.
