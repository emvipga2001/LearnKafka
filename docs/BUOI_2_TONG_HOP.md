# 📓 SỔ TAY TỔNG HỢP KIẾN THỨC - BUỔI HỌC SỐ 2
*Khóa học: Làm Chủ Apache Kafka & Java (Mastery)*  
*Chặng đường: Module 2 (Partitioner, Serializer) & Module 3 (Consumer Architecture)*

---

## 🌟 PHẦN 1: CUSTOM PARTITIONER & BÀI TOÁN "HOT PARTITION" (MODULE 2 - BÀI 2)

### 1. Bài toán Noisy Neighbor & Phân luồng ưu tiên
- Mặc định Kafka băm `Murmur2(key)` chia đều tin nhắn, dẫn tới nguy cơ đơn hàng của khách hàng VIP bị kẹt chung với hàng triệu đơn hàng của khách hàng thường trong ngày Sale.
- Giải pháp: Cài đặt interface `org.apache.kafka.clients.producer.Partitioner`:
  - Khách hàng có Key `VIP-*`: Cấp làn riêng đi thẳng vào `Partition 0`.
  - Khách hàng thường: Băm đều vào các partition còn lại (`1`, `2`...).

### 2. Hiện tượng Hot Partition (Partition Hotspotting / Data Skew)
- Khi lượng dữ liệu đổ dồn quá mức vào một partition duy nhất:
  - Gây quá tải CPU và Disk I/O trên Broker nắm giữ Partition Leader đó.
  - Tạo nút thắt cổ chai cho Consumer vì mỗi partition chỉ có tối đa 1 Consumer trong nhóm được đọc cùng lúc.
- Cách phòng tránh: Chia dải (pool/range) gồm nhiều partition dành riêng cho dữ liệu lưu lượng lớn và tiếp tục băm hash trên dải đó.

---

## 🌟 PHẦN 2: SERIALIZATION CHUYÊN SÂU & MODEL JAVA RECORD (MODULE 2 - BÀI 3)

### 1. Bản chất của Serializer trong Kafka
- Broker và mạng chỉ hiểu mảng byte (`byte[]`). Quá trình biến Java Object thành `byte[]` diễn ra **trên Main Thread** của ứng dụng Java tại thời điểm gọi `producer.send()`.
- Nếu quá trình chuyển đổi thất bại, ngoại lệ `SerializationException` được ném ra ngay lập tức tại Main Thread trước khi bất kỳ gói tin mạng nào được gửi đi.

### 2. Mã nguồn đã hoàn thiện:
- `OrderEvent.java`: Dùng Java 21 `record` bất biến (Immutable), Type-Safe.
- `JsonSerializer.java`: Generic Serializer `<T>` với Jackson `ObjectMapper`, hỗ trợ định dạng ISO-8601 cho `Instant` (JSR-310) và xử lý `null` cho Tombstone.
- `OrderProducer.java`: Producer chuẩn doanh nghiệp gửi Object type-safe với Callback.

---

## 🌟 PHẦN 3: KIẾN TRÚC NỘI TẠI CỦA KAFKA CONSUMER (MODULE 3 - BÀI 1)

### 1. Triết lý PULL (Kéo) vs PUSH (Đẩy)
- Khác với RabbitMQ (Push ồ ạt dễ gây OOM khi thiếu Backpressure), Kafka sử dụng mô hình PULL (Buffet tự chọn):
- Consumer chủ động gọi `poll()` xin lượng tin nhắn vừa sức (`max.poll.records`), tự nhiên đạt được cơ chế Backpressure.

### 2. Giao thức Phối hợp Nhóm (Group Coordination Protocol)
- **Tìm Coordinator**: Tính toán `Math.abs(groupId.hashCode()) % 50` để xác định partition trong topic nội bộ `__consumer_offsets`. Broker là Leader của partition đó sẽ trở thành **Group Coordinator**.
- **JoinGroup & SyncGroup**: Coordinator chọn Consumer đầu tiên làm **Group Leader**. Chính Consumer Leader phía Client chạy thuật toán phân bổ Partition (`Range`, `RoundRobin`, `CooperativeSticky`) rồi gửi bản đồ phân chia lại cho Coordinator phát tán cho toàn nhóm.

### 3. Bí mật Topic nội bộ `__consumer_offsets`
- Offset commit không lưu ở MySQL hay file riêng lẻ, mà được lưu như một message bình thường vào topic nội bộ `__consumer_offsets`:
  - Key: `[GroupId, Topic, Partition]`
  - Value: `[Offset, CommitTimestamp]`
- Nhờ vậy Kafka xử lý hàng triệu commit/giây với tốc độ Append-Only Commit Log.

### 4. Kỹ thuật Long Polling & Graceful Shutdown
- `poll()` áp dụng Long Polling: Nếu chưa có dữ liệu, Broker giữ kết nối TCP trong khoảng `fetch.max.wait.ms` (500ms) trước khi trả về rỗng, giúp phản hồi tức thì khi có message mới nhưng tốn 0% CPU khi rảnh rỗi.
- `consumer.wakeup()` là phương thức duy nhất an toàn đa luồng, dùng biến cờ nguyên tử `AtomicBoolean` để đánh thức luồng `poll()` đang ngủ và ném `WakeupException` để đóng socket an toàn (`consumer.close()`).

---

## 💻 DANH MỤC FILE TRONG LẦN COMMIT NÀY:
- `src/main/java/com/learnkafka/model/OrderEvent.java`
- `src/main/java/com/learnkafka/serializer/JsonSerializer.java`
- `src/main/java/com/learnkafka/producer/partitioner/VipOrderPartitioner.java`
- `src/main/java/com/learnkafka/producer/VipProducerTest.java`
- `src/main/java/com/learnkafka/producer/OrderProducer.java`
- `src/main/java/com/learnkafka/consumer/BasicConsumer.java`
- `docs/BUOI_2_TONG_HOP.md`
- Cập nhật quy tắc giảng dạy: `AGENTS.md` và `.agents/rules/kafka-mentor.md`
