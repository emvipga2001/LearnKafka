package com.learnkafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * =========================================================================================
 * BÀI HỌC: ĐỘ TIN CẬY & KHẢ NĂNG CHỊU LỖI (CHẶNG 4 - LAB FAILOVER TEST)
 * =========================================================================================
 * Producer chuẩn Production với "Bộ tứ kim cương" chống mất dữ liệu:
 * 1. acks = all
 * 2. enable.idempotence = true
 * 3. retries = Integer.MAX_VALUE
 * 4. bootstrap.servers = trỏ vào cả 3 Brokers (localhost:9092, 9093, 9094)
 * 
 * Mục tiêu thí nghiệm:
 * - Chạy chương trình này bắn message liên tục mỗi 1 giây.
 * - Mở terminal gõ: `docker stop kafka-1` (Bắn hạ Broker Leader).
 * - Quan sát xem Producer có bị văng lỗi không, hay tự động chuyển hướng sang Leader mới!
 */
public class ReliableProducerTest {

    private static final Logger log = LoggerFactory.getLogger(ReliableProducerTest.class);
    private static final String TOPIC_NAME = "ha-order-events";
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        log.info("=== [KHỞI ĐỘNG HIGH-AVAILABILITY RELIABLE PRODUCER] ===");

        Properties props = new Properties();
        
        // 1. Trỏ vào cả 3 Broker: Nếu Broker 1 sập, Client tự động kết nối qua Broker 2 hoặc 3
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. BỘ ĐÔI VÀNG VỀ ĐỘ TIN CẬY TUYỆT ĐỐI (ZERO DATA LOSS)
        // acks = all: Bắt buộc TẤT CẢ các Broker trong ISR phải ghi đĩa thành công mới trả ACK
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        // Idempotence = true: Chống duplicate khi Client retry trên đường truyền mạng
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        
        // Thời gian tối đa cho phép retry trước khi báo lỗi (2 phút)
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Đăng ký Shutdown Hook để đóng Producer êm thắm
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.warn("Đang dừng Producer và xả toàn bộ buffer...");
            running.set(false);
            producer.close();
            log.info("=== PRODUCER ĐÃ DỪNG AN TOÀN ===");
        }));

        long messageId = 1;
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

        log.info("🚀 Bắt đầu gửi dữ liệu kiểm tra High Availability (nhấn Ctrl+C để dừng)...");

        while (running.get()) {
            String key = "USER-" + (messageId % 5);
            String value = String.format("{\"orderId\": %d, \"timestamp\": \"%s\"}", 
                    messageId, LocalDateTime.now().format(timeFormatter));

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);

            long currentMsg = messageId;
            long startTime = System.currentTimeMillis();

            // Gửi bất đồng bộ với Callback
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    long latency = System.currentTimeMillis() - startTime;
                    if (exception == null) {
                        log.info("✅ [GỬI THÀNH CÔNG] Đơn #{}: Key='{}' -> Partition: {} | Offset: {} | Độ trễ: {}ms",
                                currentMsg, key, metadata.partition(), metadata.offset(), latency);
                    } else {
                        log.error("❌ [LỖI GỬI] Đơn #{}: {}", currentMsg, exception.getMessage());
                    }
                }
            });

            messageId++;

            try {
                // Gửi đều đặn mỗi 1 giây 1 tin nhắn để dễ theo dõi quá trình Failover
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
