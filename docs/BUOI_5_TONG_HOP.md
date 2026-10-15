# 📓 SỔ TAY TỔNG HỢP KIẾN THỨC - BUỔI HỌC SỐ 5
*Khóa học: Làm Chủ Apache Kafka & Java (Mastery)*  
*Chặng đường: Module 4 - Độ Tin Cậy & Khả Năng Chịu Lỗi (High Availability & Fault Tolerance)*

---

## 🌟 PHẦN 1: BẢN CHẤT KIẾN TRÚC SAO CHÉP (REPLICATION UNDER THE HOOD)

### 1. Phân biệt Rạch ròi 2 Tầng Kiến Trúc (Tránh nhầm lẫn kinh điển)
- **Tầng 1: Hạ tầng Broker (Storage & Replication)**:
  - Hoàn toàn là cơ chế **nội bộ giữa các Broker** với nhau nhằm mục đích **chống mất dữ liệu (Fault-Tolerance)** khi máy chủ vật lý bị hỏng ổ cứng, mất điện.
  - Mỗi partition được nhân bản thành nhiều bản copy (Replica) nằm rải rác trên các Broker khác nhau.
- **Tầng 2: Ứng dụng Tiêu thụ (Consumer & Consumer Group)**:
  - Là mã nguồn Java nghiệp vụ kéo dữ liệu về để xử lý (trừ tiền, gửi email).
  - Mặc định (99.9%), **Consumer CHỈ ĐỌC TỪ PARTITION LEADER**. Consumer hoàn toàn không cần biết phía sau Broker có bao nhiêu bản sao (Follower Replicas).

### 2. Follower sao chép dữ liệu từ Leader như thế nào?
- **Kiến trúc Không chia sẻ (Shared-Nothing)**: Các Broker nằm trên các máy chủ/container độc lập, có ổ cứng riêng. Follower **tuyệt đối không thể đọc trực tiếp file segment `.log`** trên ổ đĩa của Leader.
- **`ReplicaFetcherThread`**: Mỗi Broker Follower chạy luồng ngầm gửi yêu cầu `FetchRequest` định kỳ qua mạng TCP tới Leader để kéo các batch dữ liệu mới nhất. Follower hoạt động y hệt một Consumer siêu đặc quyền.
- Dữ liệu kéo sang được Leader đọc thẳng từ **Linux Page Cache trên RAM** (Zero-Copy), nên tốc độ sao chép cực kỳ nhanh, không làm nghẽn ổ đĩa Leader.

### 3. In-Sync Replicas (ISR), LEO & High Watermark (HW)
- **ISR (Tập hợp bản sao đồng bộ)**: Danh sách các Broker đang bám đuổi sát nút với Leader. Nếu một Follower không gửi `FetchRequest` về Leader quá `replica.lag.time.max.ms` (mặc định 30s), nó sẽ bị trục xuất khỏi ISR.
- **LEO (Log End Offset)**: Tọa độ offset của bản ghi tiếp theo sẽ được ghi vào đĩa của Broker đó.
- **High Watermark (HW - Mực nước an toàn)**: Mốc offset cao nhất mà **tất cả các thành viên trong ISR đều đã ghi đĩa xong**.
- **Nguyên tắc bảo vệ**: Consumer **CHỈ ĐƯỢC ĐỌC ĐẾN MỐC HIGH WATERMARK** để ngăn chặn đọc phải dữ liệu ma (Phantom Read) lỡ khi Leader sập trước khi kịp sao chép.

---

## 🌟 PHẦN 2: BỘ TỨ KIM CƯƠNG CHO KIẾN TRÚC ZERO DATA LOSS

Để đảm bảo hệ thống không bao giờ mất 1 byte dữ liệu trong môi trường doanh nghiệp:
$$\text{Zero Data Loss} = (\text{replication.factor}=3) + (\text{min.insync.replicas}=2) + (\text{acks}=\text{all}) + (\text{unclean.leader.election}=\text{false})$$

| Cấu hình | Giá trị chuẩn | Ý nghĩa bản chất |
| :--- | :---: | :--- |
| `replication.factor` | `3` | Duy trì 3 bản copy trên 3 máy chủ khác nhau. |
| `min.insync.replicas` | `2` | Số bản sao tối thiểu trong ISR phải xác nhận ghi đĩa thành công thì Leader mới chấp nhận request. |
| `acks` (Producer) | `all` (`-1`) | Bắt buộc Leader phải chờ toàn bộ các thành viên trong ISR ghi đĩa xong mới trả ACK. |
| `unclean.leader.election.enable` | `false` | Cấm không cho phép Follower bị văng khỏi ISR lên làm Leader, ngăn chặn mất mát dữ liệu (Data Loss). |

---

## 🌟 PHẦN 3: CÁC HIỆN TƯỢNG VÀ LỖI KINH ĐIỂN TRONG HỆ THỐNG PHÂN TÁN

### 1. `NotEnoughReplicasException` vs `LeaderNotAvailableException`
- **`NotEnoughReplicasException`**: Xảy ra khi Producer gửi `acks = all` nhưng $\text{Số lượng ISR hiện tại} < \text{min.insync.replicas}$. Broker từ chối ghi dữ liệu để bảo vệ độ an toàn.
- **`LeaderNotAvailableException`**: Xảy ra khi partition tạm thời (50-200ms) hoặc vĩnh viễn không có ai làm Leader (ví dụ Leader sập mà các Follower còn sống đều không đủ điều kiện ISR).

### 2. Bầu cử Leader ưu tiên (Preferred Leader Election)
- Khi Broker Leader cũ sống lại, nó gia nhập với tư cách Follower trước. Sau khi bắt kịp dữ liệu và vào lại ISR, cơ chế `auto.leader.rebalance.enable = true` sẽ tự động chuyển quyền Leader trở lại cho nó để cân bằng tải, tránh hiện tượng dồn tải cục bộ (Hotspot Broker).

### 3. Chuyển giao Leader có gây Consumer Rebalance không?
- **KHÔNG!** Consumer Group Rebalance thuộc tầng ứng dụng (thêm/bớt Consumer).
- Khi Broker Leader sập, Consumer chỉ nhận lỗi tạm thời `NotLeaderOrFollowerException`, tự động xin Metadata mới và kết nối sang Leader mới trong vài mili-giây mà không hề làm gián đoạn cả nhóm.

---

## 💻 MÃ NGUỒN VÀ TÀI NGUYÊN ĐÃ THIẾT LẬP:
- 🐳 [`docker-compose.yml`](file:///D:/Work/Projects/LearnKafka/docker-compose.yml): Cụm 3 Broker KRaft (`kafka-1:9092`, `kafka-2:9093`, `kafka-3:9094`) + Kafka UI.
- ☕ [`src/main/java/com/learnkafka/producer/ReliableProducerTest.java`](file:///D:/Work/Projects/LearnKafka/src/main/java/com/learnkafka/producer/ReliableProducerTest.java): Producer chuẩn Production kiểm chứng Failover.
- 📓 [`docs/BUOI_5_TONG_HOP.md`](file:///D:/Work/Projects/LearnKafka/docs/BUOI_5_TONG_HOP.md)

---

## 🎯 BÀI HỌC TIẾP THEO:
👉 **CHẶNG 5: GIAO DỊCH TRONG KAFKA (KAFKA TRANSACTIONS & EXACTLY-ONCE SEMANTICS - EOS)**
- Sự khác biệt: At-most-once, At-least-once, và Exactly-once.
- Mô hình Read-Process-Write phân tán.
- Giao thức 2-Phase Commit (2PC) và Topic nội bộ `__transaction_state`.
- Cách cấu hình `transactional.id` và `isolation.level = read_committed`.
