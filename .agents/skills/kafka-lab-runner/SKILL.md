---
name: kafka-lab-runner
description: >-
  Cung cấp hướng dẫn, lệnh thực thi và script để chạy môi trường thực hành Apache Kafka cục bộ (Docker Compose KRaft, Kafka UI, Kafka CLI, Maven).
  Sử dụng skill này khi cần khởi động Kafka, tạo Topic, kiểm tra Offset/Consumer Group, gửi/nhận message qua CLI hoặc build code Java.
---

# Kafka Lab Runner & CLI Guide

Skill này giúp người thầy và học viên quản lý môi trường Lab thực hành nhanh chóng và chuẩn xác.

---

## 1. Môi Trường Docker Compose (KRaft Mode + Kafka UI)

File `docker-compose.yml` tại thư mục gốc sử dụng Kafka phiên bản mới nhất chạy chế độ **KRaft (Kafka Raft Metadata)** không cần ZooKeeper:
- **Broker Port**: `localhost:9092` (cho Java Client bên ngoài Docker)
- **Internal Port**: `kafka:29092` (cho các container nội bộ)
- **Kafka UI**: `http://localhost:8080` (giao diện web trực quan xem Topics, Partitions, Messages, Consumer Groups)

### Các lệnh quản lý Docker:
```powershell
# Khởi động cụm Kafka & UI chạy ngầm
docker compose up -d

# Xem log của Kafka broker
docker compose logs -f kafka

# Kiểm tra trạng thái các container
docker compose ps

# Dừng và xóa container giữ nguyên volume
docker compose down

# Dừng và xóa sạch dữ liệu thực hành để làm lại từ đầu
docker compose down -v
```

---

## 2. Các Lệnh Kafka CLI Thường Dùng (Bên trong Container)

Để chạy CLI, dùng `docker compose exec -it kafka <lệnh>`:

### 2.1. Quản lý Topic
```powershell
# Tạo Topic có 3 Partitions và Replication Factor = 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-events --partitions 3 --replication-factor 1

# Xem danh sách Topic
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Xem chi tiết cấu hình và phân bổ partition của Topic
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic order-events

# Xóa Topic
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic order-events
```

### 2.2. Kiểm tra Message qua Console Producer / Consumer
```powershell
# Gửi message với Key:Value từ terminal (phân cách bằng dấu hai chấm :)
docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic order-events --property "parse.key=true" --property "key.separator=:"

# Đọc toàn bộ message từ đầu cùng với Key, Value, Partition và Offset
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order-events --from-beginning --property print.key=true --property print.value=true --property print.partition=true --property print.offset=true
```

### 2.3. Giám sát Consumer Group & Lag
```powershell
# Xem danh sách các Consumer Group đang hoạt động
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Xem chi tiết độ trễ (LAG), Current-Offset, Log-End-Offset của Consumer Group
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing-group

# Reset Offset về đầu (--to-earliest) để đọc lại dữ liệu (chỉ chạy khi group đã dừng)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-processing-group --reset-offsets --to-earliest --execute --topic order-events
```

---

## 3. Thực Thi Code Java với Maven

Dự án dùng Maven và Java 21:
- Lệnh chạy class Java trực tiếp bằng Maven:
```powershell
# Biên dịch project
& "C:\Program Files\JetBrains\IntelliJ IDEA 2025.3.2\plugins\maven\lib\maven3\bin\mvn.cmd" clean compile

# Chạy một Main class cụ thể
& "C:\Program Files\JetBrains\IntelliJ IDEA 2025.3.2\plugins\maven\lib\maven3\bin\mvn.cmd" exec:java -Dexec.mainClass="com.learnkafka.producer.BasicProducer"
```
Hoặc người học có thể chạy trực tiếp bằng nút Run/Debug (Shift + F10) trong IntelliJ IDEA.
