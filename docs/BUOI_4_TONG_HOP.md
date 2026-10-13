# 📓 SỔ TAY TỔNG HỢP KIẾN THỨC - BUỔI HỌC SỐ 4
*Khóa học: Làm Chủ Apache Kafka & Java (Mastery)*  
*Chặng đường: Module 3 - Consumer Rebalance, Concurrency Deep-Dive & Đa Luồng Trong Hệ Thống*

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
- **Cooperative Sticky Rebalance (Giao thức mới)**: Chỉ chuyển giao đúng partition bị thay đổi. Các partition khác vẫn tiếp tục xử lý liên tục 100%, loại bỏ hoàn toàn Stop-The-World.

### 3. Vòng đời Callback trong `ConsumerRebalanceListener`
- **`onPartitionsRevoked()`**: Được gọi ngay trước khi nhường partition. **BẮT BUỘC dùng `commitSync(currentOffsets)`** để tạo một rào chắn đồng bộ (Synchronous Barrier), đảm bảo Broker ghi nhận mốc đọc trước khi chìa khóa được trao cho Consumer mới.
- **`onPartitionsAssigned()`**: Được gọi khi được nhận thêm partition mới.
- **`onPartitionsLost()`**: Được gọi khi Consumer bị Broker trục xuất đột ngột (Zombie Consumer). **TUYỆT ĐỐI KHÔNG commit** vì partition đã được giao cho người khác; chỉ giải phóng tài nguyên cục bộ (close DB, clear cache).

---

## 🌟 PHẦN 2: TỔNG HỢP CÂU HỎI CHUYÊN SÂU & GIẢI MÃ ĐA LUỒNG (CONCURRENCY)

### 1. Bình thường khởi tạo Consumer có cần đăng ký Listener không?
- **Không bắt buộc** nếu dùng Auto-commit (`enable.auto.commit=true`), vì mã nguồn Kafka tự xử lý ngầm (dù tiềm ẩn rủi ro trùng lặp).
- **BẮT BUỘC** khi dùng Manual Commit trong Production để tránh xử lý trùng hàng loạt đơn hàng khi có thành viên ra/vào nhóm.
- Trong Spring Boot (`@KafkaListener`), Spring Framework đã tự động tích hợp sẵn các Listener này dưới nền.

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

## 💻 MÃ NGUỒN HOÀN THÀNH:
- [`src/main/java/com/learnkafka/consumer/RebalanceConsumer.java`](file:///D:/Work/Projects/LearnKafka/src/main/java/com/learnkafka/consumer/RebalanceConsumer.java)
- [`docs/BUOI_4_TONG_HOP.md`](file:///D:/Work/Projects/LearnKafka/docs/BUOI_4_TONG_HOP.md)

---

## 🎯 BÀI HỌC TIẾP THEO:
👉 **Module 3 - Bài 4: Mô Hình Đa Luồng Nâng Cao Với Consumer (Worker Thread Pool & Key-Ordering)** hoặc bước sang **Module 4: Độ Tin Cậy & Khả Năng Chịu Lỗi (Replication Factor, ISR, Min.insync.replicas)**.
