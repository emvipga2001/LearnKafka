# 📓 SỔ TAY TỔNG HỢP KIẾN THỨC - BUỔI HỌC SỐ 3
*Khóa học: Làm Chủ Apache Kafka & Java (Mastery)*  
*Chặng đường: Module 3 - Java Consumer, Consumer Group, Distributed Architecture & Manual Offset Commit*

---

## 🌟 PHẦN 1: BẢN CHẤT GIAO THỨC CONSUMER GROUP & DÒNG ĐỜI OFFSET

### 1. Phân tích 4 Giai đoạn Khởi động của Kafka Consumer
- **Giai đoạn 1: Tìm Coordinator & Hiện tượng `NOT_COORDINATOR`**:
  - Khi Consumer Group mới xuất hiện, Broker cần chuẩn bị phân vùng tương ứng trong topic nội bộ `__consumer_offsets`. Trong tích tắc đó, Broker trả về `NOT_COORDINATOR`. Cơ chế tự phục hồi của Kafka Client sẽ tự động thử lại sau vài mili-giây.
- **Giai đoạn 2: Gia nhập nhóm (`JoinGroup`)**:
  - Coordinator cấp phát `memberId` duy nhất và tăng `generationId` để phòng chống lỗi Zombie Consumer.
- **Giai đoạn 3: Phân bổ Partition (`RangeAssignor`)**:
  - Consumer Leader phía Client tự tính toán phân bổ Partition rồi đồng bộ qua `SyncGroup`.
- **Giai đoạn 4: Tra cứu Offset & `auto.offset.reset`**:
  - Cấu hình `earliest` **chỉ có tác dụng** khi: (1) Nhóm chưa từng có commit offset nào trên `__consumer_offsets` VÀ (2) Partition có sẵn dữ liệu. Nếu container Kafka bị xóa tạo mới (Topic rỗng), `earliest` sẽ đứng đợi ở Offset 0 chứ không có message để đọc.

### 2. Phân biệt Bản chất: Topic vs Consumer Group
- **Consumer đơn lẻ**: Một tiến trình Java (`KafkaConsumer`).
- **Consumer Group**: Tập thể các Consumer hợp tác chia tải.
  - *Cùng Group (Load Balancing)*: Chia nhau các partition, mỗi partition tối đa 1 consumer đọc cùng lúc.
  - *Khác Group (Pub/Sub Fan-out)*: Mỗi group nhận trọn vẹn 1 bản copy của toàn bộ Topic với mốc Offset độc lập.
- **Thử nghiệm đổi tên**:
  - Đổi tên Topic: Mở sang tủ khác rỗng $\rightarrow$ Không có tin nhắn.
  - Đổi tên `group.id`: Nhóm mới toanh chưa có offset $\rightarrow$ Tua về Offset 0 đọc lại toàn bộ lịch sử!

---

## 🌟 PHẦN 2: KIẾN TRÚC HỆ THỐNG PHÂN TÁN (SAGA PATTERN & TRACE LOG)

### 1. Xử lý Bất đồng bộ Tốc độ (Choreography & Eventual Consistency)
- Các Consumer Group hoạt động hoàn toàn độc lập (Decoupled). Service gửi Email tốn 5ms, Service Trừ tiền tốn 2s không bao giờ làm nghẽn nhau.
- **Thiết kế đúng**: Áp dụng **Chuỗi sự kiện nhân quả (Event Chaining)**: `order-created` $\rightarrow$ Trừ tiền thành công bắn `payment-completed` $\rightarrow$ Trừ kho thành công bắn `order-confirmed` $\rightarrow$ Mới gửi Email. Không cho các service cùng đọc chung 1 event ban đầu để tránh gửi email chúc mừng trước khi trừ tiền.

### 2. Xử lý Rollback: Saga Pattern & Giao dịch Bù trừ (Compensating Transaction)
- Trong hệ thống phân tán, không có lệnh `ROLLBACK` SQL quay ngược thời gian. Ta giải quyết bằng **Giao dịch Bù trừ**: thực hiện một hành động ngược lại để cân bằng (Ví dụ: Chuyển hoàn 500k vào ví khi kho hết hàng).
- **2 Phong cách Saga**:
  - *Choreography*: Các service tự biên tự diễn, lắng nghe event của nhau (như ban nhạc jazz không có nhạc trưởng).
  - *Orchestration*: Có một service `SagaOrchestrator` đóng vai trò nhạc trưởng điều phối máy trạng thái (State Machine).
- **Hệ thống phòng thủ 3 tầng khi bù trừ thất bại**:
  - *Tầng 1*: Retry bền bỉ với tính lũy thừa (Idempotence).
  - *Tầng 2*: Hàng đợi thư chết Dead Letter Queue (DLQ) & cảnh báo On-call.
  - *Tầng 3*: Bảng đối soát tự động & Nhân viên CSKH can thiệp.

### 3. Distributed Tracing (Trace Log) & Lợi ích Sống còn
- **Ẩn dụ**: Mã vận đơn Shopee Express dán trên gói hàng đi qua nhiều kho bãi.
- **Cơ chế kỹ thuật**:
  - API Gateway sinh ra `TraceId` duy nhất.
  - Đính kèm vào **Kafka Record Headers** (`X-Trace-Id`).
  - Consumer đọc Header và nạp vào SLF4J **MDC (Mapped Diagnostic Context)** để mọi dòng log đều tự động mang theo `TraceId`.
- **4 Lợi ích**:
  1. *Root Cause Analysis*: Tìm ra nguyên nhân lỗi trong 30 giây giữa hàng triệu dòng log.
  2. *Performance Bottleneck*: Phân tích độ trễ từng mắt xích trên biểu đồ thác nước (Waterfall Graph).
  3. *Service Dependency*: Tự động vẽ sơ đồ kiến trúc luồng dữ liệu.
  4. *Customer Support*: Tra cứu toàn bộ lịch sử đơn hàng minh bạch khi khách khiếu nại.

---

## 🌟 PHẦN 3: LÀM CHỦ MANUAL OFFSET COMMIT (MODULE 3 - BÀI 2)

### 1. Hiểm họa của `enable.auto.commit = true`
- **Mất dữ liệu (Data Loss)**: Kéo 100 tin, mới xử lý 10 tin thì auto-commit mốc 100. Máy chủ sập $\rightarrow$ 90 tin còn lại mất trắng!
- **Trùng dữ liệu (Data Duplication)**: Xử lý xong 100 tin trong 3 giây nhưng chưa kịp tới mốc 5 giây auto-commit thì sập $\rightarrow$ Khi bật lại đọc và trừ tiền lại 100 đơn!

### 2. So sánh `commitSync()` vs `commitAsync()`
- `commitSync()`: Chặn luồng chính (Blocking), đợi Broker xác nhận, tự động retry khi gặp lỗi tạm thời. An toàn tuyệt đối nhưng throughput giảm.
- `commitAsync()`: Bất đồng bộ (Non-blocking), không chặn luồng, tuyệt đối KHÔNG tự retry để tránh ghi đè mốc cũ lên mốc mới.
- **Công thức chuẩn Production**: Dùng `commitAsync()` trong vòng lặp thông thường để tối đa tốc độ + Dùng `commitSync()` một lần chốt sổ trong khối `finally` trước khi `consumer.close()`.

### 3. Chuẩn At-Least-Once Semantics & Đánh đổi Hiệu năng
- Mô hình: **Xử lý xong dữ liệu $\rightarrow$ Mới gọi commit**. Đảm bảo zero data loss. Kết hợp khóa chính / Unique Index ở Database để chống xử lý trùng lặp.
- **Cái bẫy của Commit Per-Record**: Gọi `commitSync()` sau mỗi bản ghi làm tụt 99% throughput do chi phí chờ mạng (Network Round-Trip Latency).
- **Giải pháp tối ưu**: Commit theo bước nhảy (Batching Chunk - ví dụ cứ 50 đơn commit 1 lần) hoặc dùng `commitSync(Map<TopicPartition, OffsetAndMetadata>)`.

---

## 💻 MÃ NGUỒN HOÀN THÀNH TRONG BUỔI HỌC:
- 👉 [`src/main/java/com/learnkafka/consumer/BasicConsumer.java`](file:///c:/Users/nguye/IdeaProjects/LearnKafka/src/main/java/com/learnkafka/consumer/BasicConsumer.java)
- 👉 [`src/main/java/com/learnkafka/consumer/ManualCommitConsumer.java`](file:///c:/Users/nguye/IdeaProjects/LearnKafka/src/main/java/com/learnkafka/consumer/ManualCommitConsumer.java)
- 👉 [`docs/BUOI_3_TONG_HOP.md`](file:///c:/Users/nguye/IdeaProjects/LearnKafka/docs/BUOI_3_TONG_HOP.md)
