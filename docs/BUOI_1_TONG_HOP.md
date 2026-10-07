# 📓 SỔ TAY TỔNG HỢP KIẾN THỨC - BUỔI HỌC SỐ 1
*Khóa học: Làm Chủ Apache Kafka & Java (Mastery)*  
*Thời gian hoàn thành: Buổi 1 (Module 1 & Module 2 - Bài 1)*

---

## 🌟 PHẦN 1: BẢN CHẤT KIẾN TRÚC PHÂN TÁN (MODULE 1)

### 1. Tại sao Kafka ra đời? (Event Streaming vs Message Queue)
- **Vấn đề Spaghetti Architecture**: Trong hệ thống Microservices, gọi HTTP REST trực tiếp giữa các service gây nên gắn kết chặt (tight coupling) và sập dây chuyền (cascading failure).
- **So sánh RabbitMQ vs Kafka**:
  - *RabbitMQ (Hộp thư bưu điện)*: Message bị xóa ngay sau khi Consumer đọc xong (Ack). Khó replay dữ liệu. Tối ưu cho queue tác vụ nhỏ.
  - *Kafka (Cuốn nhật ký Append-Only)*: Dữ liệu ghi nối tiếp và lưu trữ bền vững trên đĩa cứng (mặc định 7 ngày hoặc vĩnh viễn). Consumer đọc xong tin nhắn vẫn còn nguyên. Cho phép nhiều service khác nhau đọc lại toàn bộ lịch sử tùy ý.

### 2. Ba Bí Mật Giúp Kafka Nhanh Vượt Trội (Under The Hood)
1. **Append-Only Commit Log & Sequential I/O (Ghi đĩa tuần tự)**:
   - Kafka không bao giờ sửa đổi hoặc xóa lẻ dữ liệu cũ (tránh Random I/O).
   - Ghi tuần tự nối đuôi giúp đầu đọc đĩa không phải nhảy vị trí (Seek time = 0), tốc độ ghi đạt hàng trăm MB/s, nhanh ngang RAM.
2. **OS Page Cache (Không phụ thuộc JVM Heap)**:
   - Kafka để JVM heap nhỏ, nhường việc cache dữ liệu cho Kernel của Hệ điều hành (Page Cache).
   - Tránh hiện tượng Stop-The-World của Java Garbage Collection (GC) khi dữ liệu lên tới hàng chục GB.
3. **Zero-Copy Network Transfer**:
   - Dùng system call `sendfile()` của Linux (`FileChannel.transferTo()` trong Java NIO).
   - Dữ liệu đẩy thẳng từ OS Page Cache sang Card mạng (NIC), bỏ qua bước copy lên User Space của JVM.

### 3. Cấu Trúc Vật Lý: Topic, Partition & Segment Files
- **Partition**: Đơn vị chia nhỏ của Topic để scale-out song song. Kafka **chỉ đảm bảo thứ tự trên từng Partition**, không đảm bảo toàn bộ Topic.
- **Offset**: Tọa độ vị trí bất biến của message trong một Partition (tăng dần: 0, 1, 2...).
- **Segment Files trên ổ đĩa**: Mỗi partition chia thành các file segment (`.log`, `.index`, `.timeindex`). Khi hết hạn lưu trữ (retention), Kafka xóa thẳng nguyên cả file segment trong 1 microsecond.

---

## ⚙️ PHẦN 2: JAVA PRODUCER CHUYÊN SÂU (MODULE 2 - BÀI 1)

### 1. Ẩn dụ "Chuyến Xe Buýt Chở Khách" (Batching)
- Producer không bắn lẻ từng tin nhắn qua socket mạng vì chi phí overhead của TCP/IP rất lớn.
- Tin nhắn được gom vào **xe buýt trong RAM (RecordAccumulator)**:
  - `batch.size = 16384` (16KB): Đủ người thì xe xuất bến.
  - `linger.ms = 20` (20 mili-giây): Chưa đủ người nhưng hết thời gian chờ thì xe vẫn xuất bến.

### 2. Mô Hình 2 Luồng Độc Lập Trong KafkaProducer
- **Main Thread (Ứng dụng Java)**: Gọi `send()` -> ném message qua Serializer và Partitioner -> đẩy vào `RecordAccumulator` trong RAM rồi trả về ngay lập tức (Non-blocking).
- **Background Sender Thread (I/O Thread)**: Chạy ngầm, lấy các batch đã sẵn sàng trong RAM đóng gói thành Request gửi qua mạng tới Kafka Broker.
- **Callback (`onCompletion`)**: Được kích hoạt bởi chính Background Sender Thread khi nhận biên lai (Ack) từ Broker.

### 3. Các Cơ Chế Đã Chứng Minh Qua Log Thực Tế
- **Idempotent Producer (`enable.idempotence = true`)**: Broker tự cấp `ProducerId` (PID) và sequence number để tự động lọc bỏ các message gửi trùng khi mạng retry.
- **Thuật toán băm Murmur2**: Tự động băm `Key` để chia message vào các Partition đồng nhất. Cùng Key sẽ luôn rơi vào cùng 1 Partition.
- **Lệnh `producer.flush()`**: Bắt buộc Sender Thread xả toàn bộ message còn kẹt trong RAM sang Broker trước khi JVM tắt, chống mất mát dữ liệu.

---

## 💻 MÃ NGUỒN ĐÃ HOÀN THÀNH
- Docker KRaft cluster: [`docker-compose.yml`](file:///c:/Users/nguye/IdeaProjects/LearnKafka/docker-compose.yml)
- Java 21 Producer chuẩn Production: [`BasicProducer.java`](file:///c:/Users/nguye/IdeaProjects/LearnKafka/src/main/java/com/learnkafka/producer/BasicProducer.java)

---

## 🎯 KẾ HOẠCH CHO BUỔI HỌC TIẾP THEO
- **Module 2 - Bài 2**: Tự viết `Custom Partitioner` phân luồng dữ liệu cho khách VIP vs Standard.
- **Module 2 - Bài 3**: Serialization chuyên sâu với JSON POJO & Model Đơn Hàng thực tế.
