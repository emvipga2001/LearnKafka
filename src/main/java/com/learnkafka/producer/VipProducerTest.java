package com.learnkafka.producer;

import com.learnkafka.producer.partitioner.VipOrderPartitioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * =========================================================================
 * BÀI HỌC: CHẠY THỬ NGHIỆM CUSTOM PARTITIONER (MODULE 2 - BÀI 2)
 * =========================================================================
 * Mục tiêu kiểm chứng:
 * - Tất cả message có Key tiền tố "VIP-" phải rơi 100% vào Partition 0.
 * - Tất cả message khách thường "USER-" phải chia đều vào Partition 1 và 2.
 */
public class VipProducerTest {

    private static final Logger log = LoggerFactory.getLogger(VipProducerTest.class);
    private static final String TOPIC_NAME = "order-events";

    public static void main(String[] args) {
        log.info("Khởi động VipProducerTest với Custom Partitioner...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // -----------------------------------------------------------------
        // ĐĂNG KÝ CUSTOM PARTITIONER VÀO KAFKA PRODUCER
        // -----------------------------------------------------------------
        // Báo cho Kafka biết: "Đừng dùng Partitioner mặc định nữa, hãy dùng class này của tôi!"
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VipOrderPartitioner.class.getName());

        // Truyền tham số tùy biến vào (hàm configure() trong Partitioner sẽ nhận được)
        props.put("vip.prefix", "VIP-");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Danh sách các đơn hàng thử nghiệm gồm cả VIP và Standard
            String[][] orders = {
                    {"VIP-order-999", "{\"customer\": \"Hoang\", \"type\": \"DIAMOND\", \"total\": 5000}"},
                    {"USER-order-101", "{\"customer\": \"Binh\", \"type\": \"NORMAL\", \"total\": 200}"},
                    {"VIP-order-888", "{\"customer\": \"Lan\", \"type\": \"PLATINUM\", \"total\": 3500}"},
                    {"USER-order-102", "{\"customer\": \"Cuong\", \"type\": \"NORMAL\", \"total\": 150}"},
                    {"VIP-order-777", "{\"customer\": \"Mai\", \"type\": \"GOLD\", \"total\": 2000}"},
                    {"USER-order-103", "{\"customer\": \"Dung\", \"type\": \"NORMAL\", \"total\": 300}"}
            };

            for (String[] order : orders) {
                String key = order[0];
                String value = order[1];

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            log.info("--> [ĐÃ GỬI THÀNH CÔNG] Key='{}' ──> Partition={} | Offset={}",
                                    key, metadata.partition(), metadata.offset());
                        } else {
                            log.error("xxx [LỖI] Key='{}' thất bại: {}", key, exception.getMessage());
                        }
                    }
                });
            }

            // Xả sạch bộ đệm
            producer.flush();

        } catch (Exception e) {
            log.error("Lỗi khi chạy Producer: ", e);
        }

        log.info("Đã hoàn thành kiểm chứng VipProducerTest!");
    }
}
