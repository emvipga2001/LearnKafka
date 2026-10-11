package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * =========================================================================
 * BÀI HỌC: QUẢN LÝ OFFSET THỦ CÔNG (CHẶNG 3 - BÀI 2)
 * =========================================================================
 * BỘ KHUNG SƯỜN (CODE SKELETON)
 * 
 * Mục tiêu:
 * 1. Tắt hoàn toàn Auto-Commit để bảo vệ dữ liệu (enable.auto.commit = false).
 * 2. Phân biệt commitSync() (chắc chắn, blocking) vs commitAsync() (nhanh, non-blocking).
 * 3. Tự tay kiểm soát mốc thời gian commit sau khi nghiệp vụ hoàn tất!
 */
public class ManualCommitConsumer {

    private static final Logger log = LoggerFactory.getLogger(ManualCommitConsumer.class);
    private static final String TOPIC_NAME = "order-events";
    private static final String GROUP_ID = "order-manual-commit-group";

    public static void main(String[] args) {
        log.info("=== [KHỞI ĐỘNG CONSUMER VỚI MANUAL OFFSET COMMIT] ===");

        // -----------------------------------------------------------------
        // PHẦN 1: CẤU HÌNH (THẦY ĐÃ VIẾT SẴN 100%)
        // -----------------------------------------------------------------
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ĐIỀU KIỆN TIÊN QUYẾT: TẮT HOÀN TOÀN AUTO-COMMIT!
        // Không cho phép Kafka Client tự động lưu mốc đọc trong nền nữa.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.warn("Shutdown hook kích hoạt! Đang gọi wakeup()...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Lỗi shutdown hook: ", e);
            }
        }));

        // -----------------------------------------------------------------
        // PHẦN 2: VÒNG LẶP POLL VÀ THỬ THÁCH COMMIT THỦ CÔNG DÀNH CHO BẠN!
        // -----------------------------------------------------------------
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.info("--> Nhận được mẻ mới gồm {} bản ghi.", records.count());

                    for (ConsumerRecord<String, String> record : records) {
                        // Giả lập xử lý nghiệp vụ đơn hàng (Lưu vào Database, trừ tiền...)
                        log.info("   [ĐANG XỬ LÝ] Đơn hàng: Partition={} | Offset={} | Key='{}'",
                                record.partition(), record.offset(), record.key());
                    }

                    // 🎯 THỬ THÁCH CỦA BẠN NẰM Ở ĐÂY:
                    // Sau khi vòng lặp for duyệt và xử lý xong toàn bộ các đơn hàng trong mẻ này,
                    // Bạn hãy tự tay gọi lệnh commit thủ công để báo cho Broker biết!
                    // 
                    // TODO 1: Hãy gọi hàm commit đồng bộ (commitSync) để chặn luồng và đảm bảo
                    // Broker đã lưu mốc đọc an toàn rồi mới tiếp tục vòng lặp mới.
                    // Gợi ý: consumer.commitSync();
                    //
                    // >>> GÕ LỆNH COMMIT CỦA BẠN DƯỚI ĐÂY: <<<
                    consumer.commitSync();

                    log.info("<-- Đã commit offset thành công cho mẻ vừa xử lý!");
                }
            }

        } catch (WakeupException e) {
            log.info("Nhận tín hiệu dừng từ WakeupException.");
        } catch (CommitFailedException e) {
            log.error("Lỗi Commit thất bại (do Rebalance hoặc xử lý quá thời gian max.poll.interval.ms): ", e);
        } catch (Exception e) {
            log.error("Lỗi không mong muốn: ", e);
        } finally {
            try {
                // 🎯 THỬ THÁCH NÂNG CAO:
                // TODO 2: Trước khi đóng vĩnh viễn consumer.close(), ta nên làm một cú commit đồng bộ
                // cuối cùng để vét cạn những mốc chưa kịp lưu.
                // Gợi ý: consumer.commitSync();
                //
                // >>> GÕ LỆNH COMMIT CUỐI CÙNG DƯỚI ĐÂY: <<<
                consumer.commitSync();

            } finally {
                log.info("Đang đóng consumer...");
                consumer.close();
                log.info("=== CONSUMER ĐÃ ĐÓNG HOÀN TOÀN ===");
            }
        }
    }
}
