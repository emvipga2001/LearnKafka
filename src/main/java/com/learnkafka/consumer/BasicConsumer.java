package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * =========================================================================================
 * BÀI HỌC CHUYÊN SÂU: KIẾN TRÚC POLL LOOP & QUẢN LÝ OFFSET CỐT TỬ CỦA KAFKA CONSUMER
 * =========================================================================================
 * 
 * Class này mô phỏng luồng tiêu thụ dữ liệu chuẩn Production với:
 * 1. Phân tích cặn kẽ từng tham số sống còn (max.poll.records, fetch.min.bytes, auto.commit).
 * 2. Vòng lặp poll() xử lý từng message với đo lường thời gian thực.
 * 3. Bắt ngoại lệ và cơ chế Graceful Shutdown thông qua WakeupException.
 */
public class BasicConsumer {

    private static final Logger log = LoggerFactory.getLogger(BasicConsumer.class);
    private static final String TOPIC_NAME = "order-events";
    private static final String GROUP_ID = "order-processing-group-v2";

    public static void main(String[] args) {
        log.info("=== [KHỞI ĐỘNG KAFKA CONSUMER CHUYÊN SÂU] ===");

        // -----------------------------------------------------------------
        // 1. CẤU HÌNH CỐT TỬ DƯỚI TẦNG NỘI TẠI (UNDER-THE-HOOD CONFIGS)
        // -----------------------------------------------------------------
        Properties props = new Properties();

        // [KẾT NỐI MẠNG]
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // [CONSUMER GROUP & PHÂN CHIA TẢI]
        // Group ID định danh nhóm người đọc. Kafka dựa vào chuỗi này để tìm Broker đóng vai trò Group Coordinator.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // [DESERIALIZATION]
        // Chuyển mảng byte[] nhận từ socket TCP thành String trong JVM Heap.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // [VỊ TRÍ BẮT ĐẦU ĐỌC - AUTO OFFSET RESET]
        // Nếu Consumer Group này LẦN ĐẦU TIÊN xuất hiện (chưa có offset lưu trong __consumer_offsets):
        // - "earliest": Tua lại đọc từ offset 0 (từ đầu lịch sử lưu trữ của Partition).
        // - "latest" (mặc định): Bỏ qua toàn bộ tin nhắn cũ, chỉ nghe các tin nhắn gửi tới SAU KHI consumer khởi động.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // [TỐI ƯU HÓA THÔNG LƯỢNG & BACKPRESSURE - BUFFER POOL]
        // max.poll.records: Giới hạn số lượng bản ghi tối đa được lấy về trong 1 lần gọi poll().
        // Giúp kiểm soát bộ nhớ RAM của ứng dụng, tránh OOM nếu tin nhắn có kích thước lớn.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        // [LONG-POLLING VỚI BROKER]
        // fetch.min.bytes: Broker chỉ gửi dữ liệu về khi gom đủ ít nhất 1 byte (phản hồi tức thì khi có dữ liệu).
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        // fetch.max.wait.ms: Thời gian tối đa Broker được phép chờ gom dữ liệu trước khi bắt buộc trả lời Consumer (mặc định 500ms).
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // [QUẢN LÝ OFFSET - CƠ CHẾ TỰ ĐỘNG COMMIT]
        // Ở bài này ta tạm bật auto-commit để quan sát luồng cơ bản. 
        // Bài sau thầy sẽ chỉ cho bạn hiểm họa mất dữ liệu của nó và cách tự commit bằng tay (Manual Commit)!
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000"); // 5 giây commit 1 lần

        // -----------------------------------------------------------------
        // 2. KHỞI TẠO INSTANCE VÀ ĐĂNG KÝ TOPIC
        // -----------------------------------------------------------------
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Đăng ký Topic: Hành động này kích hoạt giao thức Rebalance. Consumer sẽ gửi request
        // "FindCoordinator" và "JoinGroup" lên cụm Broker.
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        log.info("Consumer đã đăng ký lắng nghe topic '{}' với GroupId='{}'", TOPIC_NAME, GROUP_ID);

        // -----------------------------------------------------------------
        // 3. THIẾT LẬP CƠ CHẾ TẮT TIẾN TRÌNH AN TOÀN (GRACEFUL SHUTDOWN HOOK)
        // -----------------------------------------------------------------
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.warn(">>> [SHUTDOWN HOOK] Nhận tín hiệu dừng từ hệ điều hành (SIGINT/SIGTERM)!");
            // wakeup() là phương thức DUY NHẤT an toàn đa luồng của KafkaConsumer.
            // Nó đặt cờ ngắt trong Consumer. Khi poll() đang bị block chờ mạng, nó sẽ lập tức
            // ném ra WakeupException để thoát khỏi vòng lặp vô tận while(true).
            consumer.wakeup();
            try {
                // Chờ Main Thread dọn dẹp và đóng socket hoàn toàn
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Lỗi khi chờ Main Thread dừng: ", e);
            }
        }));

        // -----------------------------------------------------------------
        // 4. VÒNG LẶP SỰ KIỆN POLL LOOP (THE HEART OF CONSUMER)
        // -----------------------------------------------------------------
        try {
            log.info("Bắt đầu vòng lặp Poll Loop. Đang lắng nghe tin nhắn từ Kafka Broker...");

            while (true) {
                // GỌI HÀM POLL():
                // - Gửi fetch request tới Broker chứa Leader Partition.
                // - Nếu không có message nào mới, luồng sẽ ngủ tối đa 1000ms rồi tiếp tục vòng lặp mới.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.info("===> [POLL NHẬN DỮ LIỆU] Kéo thành công {} bản ghi từ Broker!", records.count());

                    for (ConsumerRecord<String, String> record : records) {
                        // In chi tiết từng tọa độ vật lý của bản ghi:
                        log.info("📦 [ĐƠN HÀNG] Partition: {} | Offset: {} | Key: '{}' | Leader Timestamp: {}",
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.timestamp());
                        log.info("    └── Dữ liệu JSON (Payload): {}", record.value());

                        // Giả lập thời gian CPU xử lý nghiệp vụ (ví dụ: bóc tách JSON, trừ tiền thẻ...)
                        // Trong thực tế, đây là nơi logic kinh doanh của bạn chạy.
                    }
                }
            }

        } catch (WakeupException e) {
            // Đây KHÔNG PHẢI là lỗi! Đây là tín hiệu thoát có chủ đích được kích hoạt bởi consumer.wakeup()
            log.info("Consumer đã bắt được WakeupException và đang chuẩn bị thoát vòng lặp an toàn.");
        } catch (Exception e) {
            log.error("Gặp lỗi ngoại lệ không mong muốn trong vòng lặp Poll: ", e);
        } finally {
            // BƯỚC ĐÓNG AN TOÀN CỐT TỬ:
            // consumer.close() sẽ làm 3 việc cực kỳ quan trọng:
            // 1. Gửi request "LeaveGroup" tới Group Coordinator để thông báo: "Tôi nghỉ rồi, 
            //    hãy chia lại các partition của tôi cho consumer khác ngay lập tức!" (Tránh rebalance timeout 45s).
            // 2. Commit các offset chưa kịp lưu nếu auto-commit đang bật.
            // 3. Đóng tất cả các Socket TCP Channel kết nối tới Broker để giải phóng tài nguyên OS.
            log.info("Đang thực hiện consumer.close()...");
            consumer.close();
            log.info("=== [CONSUMER ĐÃ ĐÓNG KẾT NỐI AN TOÀN] ===");
        }
    }
}
