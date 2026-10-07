# Khóa Học Thực Chiến Apache Kafka & Java (Mastery)

Chào mừng bạn đến với dự án học và thực hành **Apache Kafka chuyên sâu cùng Java**!
Dự án này đã được cấu hình với AI Mentor (Người thầy ảo) luôn đồng hành, giải thích chi tiết bản chất từng dòng code, cơ chế ngầm (under-the-hood) của hệ thống phân tán, và hướng dẫn bạn từng bước từ Zero đến Hero.

---

## 🛠️ Công Cụ Trong Dự Án

- **Java**: Java 21 LTS
- **Kafka Clients**: Version 3.8.0
- **Kafka Cluster**: Chế độ KRaft Mode (Apache Kafka mới nhất, không cần ZooKeeper)
- **Kafka UI**: Giao diện web trực quan tại `http://localhost:8080`
- **Build tool**: Maven (`pom.xml`)

---

## 🚀 Khởi Động Nhanh Môi Trường

### 1. Khởi động Kafka & Kafka UI
Chạy lệnh sau trong PowerShell hoặc Terminal:
```powershell
docker compose up -d
```
Sau khi chạy, truy cập [http://localhost:8080](http://localhost:8080) để vào giao diện **Kafka UI**.

### 2. Dừng môi trường
```powershell
docker compose down
```

---

## 📚 8 Chặng Đường Học Tập Cùng AI Mentor

1. **Module 1**: Bản chất kiến trúc phân tán Kafka, Sequential I/O, Zero-Copy, Commit Log & Partition.
2. **Module 2**: Java Producer chuyên sâu, RecordAccumulator, Acks, Retries, Idempotence & Custom Partitioner.
3. **Module 3**: Java Consumer chuyên sâu, Poll Loop, Offset Commit, Consumer Rebalance & Multithreading.
4. **Module 4**: Độ tin cậy & Chịu lỗi, In-Sync Replicas (ISR), Min.insync.replicas, Failover Simulation.
5. **Module 5**: Giao dịch phân tán (Kafka Transactions) & Exactly-Once Semantics (EOS).
6. **Module 6**: Error Handling, Non-blocking Retry & Dead Letter Queue (DLQ).
7. **Module 7**: Tích hợp Spring Boot 3 & Spring Kafka trong Microservices.
8. **Module 8**: Tối ưu hiệu năng (Performance Tuning), Log Compaction & Giám sát Production.

---

## 👨‍🏫 Cách Học Với Mentor

Bạn chỉ cần trò chuyện với Agent trong khung chat:
- Gõ: *"Thầy ơi, mình bắt đầu Module 1 nhé!"*
- Gõ: *"Giải thích chi tiết cho tôi đoạn code Producer này hoạt động thế nào dưới tầng OS / Network?"*
- Gõ: *"Cho tôi một bài tập thực hành về Consumer Rebalance!"*
Agent sẽ áp dụng ngay quy tắc sư phạm, mổ xẻ tường tận và cùng bạn code từng bước!
