package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * =========================================================================================
 * BÀI HỌC: CONSUMER REBALANCE PROTOCOL & REBALANCE LISTENER (CHẶNG 3 - BÀI 3)
 * =========================================================================================
 * BỘ KHUNG SƯỜN (CODE SKELETON)
 * 
 * Mục tiêu:
 * 1. Hiểu hiện tượng Rebalance xảy ra khi nào và tại sao nó gây rủi ro trùng lặp dữ liệu.
 * 2. Sử dụng chiến lược phân bổ hiện đại: CooperativeStickyAssignor (tránh Stop-The-World).
 * 3. Tự tay viết logic "Bàn giao ca trực" (ConsumerRebalanceListener) để commit an toàn trước khi bị thu hồi partition!
 */
public class RebalanceConsumer {

    private static final Logger log = LoggerFactory.getLogger(RebalanceConsumer.class);
    private static final String TOPIC_NAME = "order-events";
    private static final String GROUP_ID = "rebalance-demo-group";

    // Bảng lưu vết tọa độ đọc hiện tại của từng Partition trong bộ nhớ RAM
    // Key: TopicPartition (ví dụ: order-events-0)
    // Value: OffsetAndMetadata (mốc offset kế tiếp cần đọc: offset + 1)
    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public static void main(String[] args) {
        log.info("=== [KHỞI ĐỘNG REBALANCE CONSUMER CHUYÊN SÂU] ===");

        // -----------------------------------------------------------------
        // PHẦN 1: CẤU HÌNH (THẦY ĐÃ VIẾT SẴN 100%)
        // -----------------------------------------------------------------
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // TẮT AUTO-COMMIT để tự tay kiểm soát mốc bàn giao
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // CHIẾN LƯỢC PHÂN BỔ HIỆN ĐẠI: COOPERATIVE STICKY
        // Thay vì giao thức cũ (Eager) bắt tất cả Consumer buông tay dừng làm việc (Stop-the-world),
        // CooperativeSticky chỉ chuyển giao đúng partition bị thay đổi, các partition khác vẫn chạy bình thường!
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // -----------------------------------------------------------------
        // PHẦN 2: ĐĂNG KÝ LISTENER ĐỂ BÀN GIAO CA TRỰC KHI REBALANCE XẢY RA
        // -----------------------------------------------------------------
        consumer.subscribe(Collections.singletonList(TOPIC_NAME), new ConsumerRebalanceListener() {

            /**
             * ĐƯỢC GỌI NGAY TRƯỚC KHI CÁC PARTITION NÀY BỊ TƯỚC KHỎI TAY CONSUMER!
             * Đây là cơ hội CUỐI CÙNG để ta commit những gì mình vừa xử lý xong,
             * tránh để Consumer mới tiếp quản phải đọc lại từ đầu gây trùng lặp dữ liệu.
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn("⚠️ [REBALANCE BẮT ĐẦU] Bị thu hồi quyền sở hữu các Partitions: {}", partitions);

                // 🎯 THỬ THÁCH 1 DÀNH CHO BẠN:
                // TODO 1: Hãy gọi lệnh commit đồng bộ (commitSync) danh sách mốc đọc hiện tại (currentOffsets)
                // để chốt sổ mốc bàn giao an toàn lên Broker!
                // Gợi ý: consumer.commitSync(currentOffsets);
                //
                // >>> GÕ LỆNH COMMIT BÀN GIAO CỦA BẠN DƯỚI ĐÂY: <<<

                consumer.commitSync(currentOffsets);

                log.info("✅ [BÀN GIAO XONG] Đã commit mốc đọc an toàn cho các partition bị thu hồi.");
            }

            /**
             * ĐƯỢC GỌI NGAY SAU KHI CONSUMER ĐƯỢC PHÂN CÔNG THÊM CÁC PARTITION MỚI!
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("🎉 [TIẾP QUẢN MỚI] Được phân công quản lý thêm các Partitions: {}", partitions);
            }

            /**
             * ĐƯỢC GỌI KHI BỊ MẤT PARTITION ĐỘT NGỘT (VÍ DỤ TIMEOUT REBALANCE)
             */
            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                log.error("❌ [MẤT KẾT NỐI] Đã mất quyền sở hữu các Partitions mà không kịp thu hồi: {}", partitions);
            }
        });

        // SHUTDOWN HOOK
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.warn("Nhận tín hiệu dừng! Đang đánh thức consumer.wakeup()...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Lỗi shutdown hook: ", e);
            }
        }));

        // -----------------------------------------------------------------
        // PHẦN 3: VÒNG LẶP POLL VÀ CẬP NHẬT TỌA ĐỘ OFFSET
        // -----------------------------------------------------------------
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("   [XỬ LÝ] Partition: {} | Offset: {} | Key: '{}'",
                                record.partition(), record.offset(), record.key());

                        // 🎯 THỬ THÁCH 2 DÀNH CHO BẠN:
                        // Sau khi xử lý xong bản ghi này, ta cần cập nhật tọa độ mới nhất vào bảng currentOffsets.
                        // Lưu ý quan trọng: Mốc commit luôn phải là (record.offset() + 1) để lần sau đọc bản ghi kế tiếp!
                        //
                        // TODO 2: Cập nhật TopicPartition và OffsetAndMetadata vào currentOffsets:
                        // Gợi ý:
                        // TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        // currentOffsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
                        //
                        // >>> GÕ CODE CẬP NHẬT OFFSET CỦA BẠN DƯỚI ĐÂY: <<<
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        currentOffsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
                    }

                    // Định kỳ commit sau mỗi mẻ
                    consumer.commitAsync();
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer dừng an toàn theo tín hiệu Wakeup.");
        } catch (Exception e) {
            log.error("Lỗi trong vòng lặp poll: ", e);
        } finally {
            try {
                // Commit chốt sổ trước khi đóng
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                log.info("=== CONSUMER ĐÃ ĐÓNG HOÀN TOÀN ===");
            }
        }
    }
}
