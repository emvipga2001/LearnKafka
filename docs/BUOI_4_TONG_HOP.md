# 📓 SỔ TAY TỔNG HỢP KIẾN THỨC - BUỔI HỌC SỐ 4
*Khóa học: Làm Chủ Apache Kafka & Java (Mastery)*  
*Chặng đường: Module 3 - Consumer Rebalance, Concurrency Deep-Dive, Worker Thread Pool & Error Handling (Retry & DLQ)*

---

## 🌟 PHẦN 1: BẢN CHẤT GIAO THỨC REBALANCE & REBALANCE LISTENER (MODULE 3 - BÀI 3)

### 1. Bản chất và Nguyên nhân kích hoạt Rebalance
- **Rebalance (Tái cân bằng)**: Quá trình phân bổ lại quyền sở hữu các Partition giữa các Consumer trong cùng một Consumer Group.
- **3 Nguyên nhân chính**:
  1. *Thay đổi số lượng Consumer*: Consumer mới tham gia (`JoinGroup`), Consumer tắt chủ động (`LeaveGroup`), hoặc chết đột ngột (không gửi được Heartbeat quá `session.timeout.ms`).
  2. *Cái bẫy `max.poll.interval.ms`*: Luồng xử lý chính bị nghẽn (gọi API thứ ba quá lâu). Coordinator coi như Consumer đã bị "treo" và đuổi khỏi nhóm.
  3. *Thay đổi số lượng Partition của Topic*.

### 2. Sự tiến hóa: Eager Rebalance vs Cooperative Sticky Rebalance
- **Eager Rebalance (Giao thức cũ)**: Bắt toàn bộ Consumer buông sạch partition $\rightarrow$ Hiện tượng dừng hệ thống **Stop-The-World**, gây ra nguy cơ nghẽn mạng **Rebalance Storm**.
- **Cooperative Sticky Rebalance (Giao thức mới - KIP-429)**: Chỉ chuyển giao đúng partition bị thay đổi. Các partition khác vẫn tiếp tục xử lý liên tục 100%, loại bỏ hoàn toàn Stop-The-World.

### 3. Vòng đời Callback trong `ConsumerRebalanceListener`
- **`onPartitionsRevoked()`**: Được gọi ngay trước khi nhường partition. **BẮT BUỘC dùng `commitSync(currentOffsets)`** để tạo một rào chắn đồng bộ (Synchronous Barrier), đảm bảo Broker ghi nhận mốc đọc trước khi chìa khóa được trao cho Consumer mới. Tránh tình trạng dùng `commitAsync()` khiến gói tin commit bị trôi và Consumer mới xử lý trùng lặp dữ liệu.
- **`onPartitionsAssigned()`**: Được gọi khi được nhận thêm partition mới.
- **`onPartitionsLost()`**: Được gọi khi Consumer bị Broker trục xuất đột ngột (Zombie Consumer). **TUYỆT ĐỐI KHÔNG commit** vì partition đã được giao cho người khác; chỉ giải phóng tài nguyên cục bộ (close DB, clear cache).

---

## 🌟 PHẦN 2: TỔNG HỢP CÂU HỎI CHUYÊN SÂU & GIẢI MÃ ĐA LUỒNG (CONCURRENCY)

### 1. Khởi tạo Consumer bình thường có cần đăng ký Listener không?
- **Không bắt buộc** nếu dùng Auto-commit (`enable.auto.commit=true`), vì mã nguồn Kafka tự xử lý ngầm (dù tiềm ẩn rủi ro mất/trùng dữ liệu).
- **BẮT BUỘC** khi dùng Manual Commit trong Production để tránh xử lý trùng hàng loạt đơn hàng khi có thành viên ra/vào nhóm.
- Trong Spring Boot (`@KafkaListener`), Spring Framework đã tự động tích hợp sẵn các Listener này dưới nền (`ConsumerAwareRebalanceListener`).

### 2. Chỗ nào trong mã nguồn cấm `KafkaConsumer` chạy đa luồng?
- Trong mã nguồn Apache Kafka `KafkaConsumer.java`, mọi phương thức (`poll`, `commitSync`, `seek`...) đều gọi hàm kiểm tra:
  ```java
  private void acquire() {
      long threadId = Thread.currentThread().getId();
      if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId)) {
          throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
      }
      refcount.incrementAndGet();
  }
  ```
- Nếu 2 Thread cùng chọc vào 1 instance `KafkaConsumer` $\rightarrow$ Ném ngay lỗi `ConcurrentModificationException`.
- **Ngoại lệ duy nhất**: `consumer.wakeup()` là hàm thread-safe duy nhất để ngắt luồng poll từ bên ngoài.

### 3. Đa luồng trong một Service thông thường (Spring Boot) vs Kafka
- **Spring `@Service` thông thường (Stateless)**:
  - Bản thân Service là **Singleton** (1 instance duy nhất trên Heap).
  - 200 Thread của Tomcat cùng chạy qua Service này an toàn vì **mọi biến nghiệp vụ đều là biến cục bộ (Local Variables) nằm trên vùng nhớ Stack riêng biệt của từng Thread**. Không có `mutable instance field` trên Heap.
  - Tựa như 200 đầu bếp cùng đọc chung 1 cuốn sách công thức, mỗi người có thớt và nguyên liệu riêng trên bàn của mình.
- **`KafkaProducer` (Thread-safe)**: Có thể tiêm làm Singleton cho nhiều thread vì bên trong đã tích hợp sẵn queue đệm đa luồng (`ConcurrentLinkedQueue`, `AtomicInteger`).
- **`KafkaConsumer` (Stateful, Not Thread-safe)**: Lưu trạng thái con trỏ đọc (Offset), buffer TCP socket trực tiếp, không hỗ trợ nhiều thread cùng truy cập 1 instance.

### 4. Quy tắc phân bổ luồng và Partition trong Kafka
- **Trong cùng 1 Consumer Group**: 1 Partition tại một thời điểm **CHỈ ĐƯỢC GIAO CHO DUY NHẤT 1 CONSUMER**. Nếu số Consumer > Số Partition thì các Consumer thừa sẽ bị **Idle (ngồi chơi xơi nước)**.
- **Khác Consumer Group**: Nhiều Consumer thuộc các nhóm khác nhau **hoàn toàn có thể đọc chung 1 Partition** đồng thời (mô hình Pub/Sub Fan-out).
- **Mô hình 1 Consumer per Thread**: Mỗi Thread tự `new` một instance `KafkaConsumer` riêng biệt, cùng cấu hình chung `group.id`.

---

## 🌟 PHẦN 3: MÔ HÌNH ĐA LUỒNG NÂNG CAO - WORKER THREAD POOL & XỬ LÝ LỖI (MODULE 3 - BÀI 4)

### 1. Tại sao cần Worker Thread Pool (Decoupled Architecture)?
- Khi mỗi đơn hàng mất 200-300ms để xử lý (gọi DB, API thứ 3), mô hình 1 Consumer per Partition bị giới hạn tốc độ bởi số lượng Partition.
- **Giải pháp**: Tách rời 1 Luồng Poll (chỉ kéo dữ liệu cực nhanh) và một **Hồ luồng thợ (Worker Pool gồm nhiều Threads)** chạy song song trong RAM.

### 2. Hai Thách thức và Giải pháp Kiến trúc:
1. **Thứ tự theo Key (Key-Ordering)**: Ném ngẫu nhiên vào Worker Pool sẽ làm sự kiện `HỦY_ĐƠN` chạy trước `TẠO_ĐƠN`.
   - *Giải pháp*: **Key-Ordered Dispatcher**:
     ```java
     int workerIndex = record.key() != null ? Math.abs(record.key().hashCode()) % NUM_WORKERS : 0;
     workerQueues[workerIndex].put(record);
     ```
   - Cùng một khách hàng luôn luôn đi vào một `BlockingQueue` riêng của Worker đó.
2. **Khoảng trống Offset (Offset Gaps) & Poison Pill**:
   - Nếu một tin nhắn bị lỗi mà luồng chính tiếp tục kéo hoặc commit mốc mới, dữ liệu sẽ bị mất trắng hoặc rơi vào vòng lặp sập vô tận.

### 3. Hệ Thống Phòng Thủ Lỗi 2 Lớp: Retry Backoff & Dead Letter Queue (DLQ)
- **Lớp 1: Thử lại nội bộ (In-Memory Retry với Exponential Backoff)**:
  - Khi gặp lỗi tạm thời (mạng nhấp nháy), Worker thử lại tối đa 3 lần, mỗi lần nghỉ tăng dần (`300ms * attempt`).
- **Lớp 2: Hàng đợi thư chết (Dead Letter Queue - DLQ)**:
  - Khi đã thử hết số lần cho phép (Poison Pill), cô lập tin nhắn độc hại sang topic DLQ (`order-events-dlq`), kết thúc êm thắm để Worker tiếp tục nhận các đơn hàng phía sau.
- **Kỹ thuật Backpressure**: Dùng `consumer.pause(partitions)` khi hàng đợi bị đầy và `consumer.resume(partitions)` khi Worker đã thông thoáng, giúp luồng poll vẫn gửi heartbeat đều đặn mà không sợ tràn RAM.

### 4. Ba Bẫy Logic Kinh Điển Khi Viết Vòng Lặp Retry:
1. **Throw Exception trong block catch**: Khiến vòng lặp bị ngắt ngay ở lần đầu tiên (`attempt = 1`), vô hiệu hóa hoàn toàn cơ chế thử lại.
2. **Điều kiện biên số lần thử**: Nếu dùng `for (int i = 0; i < MAX_RETRIES; i++)`, biến `i` không bao giờ bằng `MAX_RETRIES` bên trong vòng lặp $\rightarrow$ Khối lệnh DLQ không bao giờ chạy.
3. **Thứ tự Backoff**: Phải thực thi nghiệp vụ ngay ở lần đầu. Chỉ sleep khi gặp lỗi ở các lần thử tiếp theo để không làm giảm throughput lúc hệ thống khỏe mạnh.

---

## 💻 MÃ NGUỒN ĐÃ HOÀN THÀNH TRONG BUỔI HỌC:
- 👉 [`src/main/java/com/learnkafka/consumer/RebalanceConsumer.java`](file:///D:/Work/Projects/LearnKafka/src/main/java/com/learnkafka/consumer/RebalanceConsumer.java)
- 👉 [`src/main/java/com/learnkafka/consumer/WorkerPoolConsumer.java`](file:///D:/Work/Projects/LearnKafka/src/main/java/com/learnkafka/consumer/WorkerPoolConsumer.java)
- 👉 [`docs/BUOI_4_TONG_HOP.md`](file:///D:/Work/Projects/LearnKafka/docs/BUOI_4_TONG_HOP.md)

---

## 🎯 KẾ HOẠCH CHO BUỔI HỌC TIẾP THEO (CHẶNG 4):
Chúng ta đã hoàn thành trọn vẹn Chặng 3 (Làm chủ Java Consumer). Buổi tiếp theo chúng ta sẽ tiến vào:
👉 **CHẶNG 4: ĐỘ TIN CẬY & KHẢ NĂNG CHỊU LỖI (HIGH AVAILABILITY & FAULT TOLERANCE)**
- *Replication Factor, Partition Leader & Followers*.
- *In-Sync Replicas (ISR), High Watermark (HW) và `min.insync.replicas`*.
- *Thực hành: Giả lập Broker Crash (Kill Container) và kiểm chứng Failover thời gian thực!*
