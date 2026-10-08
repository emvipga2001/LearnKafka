package com.learnkafka.producer;

import com.learnkafka.model.OrderEvent;
import com.learnkafka.model.OrderEvent.CustomerType;
import com.learnkafka.model.OrderEvent.OrderItem;
import com.learnkafka.model.OrderEvent.OrderStatus;
import com.learnkafka.serializer.JsonSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * =========================================================================
 * BÀI HỌC: ORDER PRODUCER VỚI CUSTOM JSON SERIALIZER (MODULE 2 - BÀI 3)
 * =========================================================================
 * Mục tiêu:
 * - Gửi dữ liệu hướng đối tượng hoàn toàn Type-Safe: KafkaProducer<String, OrderEvent>.
 * - Tự động biến đổi Java Record thành JSON byte[] thông qua JsonSerializer.
 */
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private static final String TOPIC_NAME = "order-events";

    public static void main(String[] args) {
        log.info("Khởi động OrderProducer với Type-Safe OrderEvent Record...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Key dùng StringSerializer thông thường
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Value dùng JsonSerializer tùy biến của chúng ta!
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // Đảm bảo an toàn tin cậy cao
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        // Khởi tạo Producer với Type rõ ràng: Key là String, Value là OrderEvent
        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(props)) {

            // Giả lập 3 đơn hàng thực tế
            List<OrderEvent> sampleOrders = List.of(
                    new OrderEvent(
                            "ORD-2026-001",
                            "CUST-8888",
                            CustomerType.VIP,
                            new BigDecimal("1250.00"),
                            List.of(
                                    new OrderItem("PROD-1", "Bàn phím cơ Custom", 1, new BigDecimal("850.00")),
                                    new OrderItem("PROD-2", "Chuột không dây Gaming", 1, new BigDecimal("400.00"))
                            ),
                            OrderStatus.CREATED,
                            Instant.now()
                    ),
                    new OrderEvent(
                            "ORD-2026-002",
                            "CUST-1111",
                            CustomerType.STANDARD,
                            new BigDecimal("45.50"),
                            List.of(
                                    new OrderItem("PROD-3", "Cáp sạc Type-C", 2, new BigDecimal("22.75"))
                            ),
                            OrderStatus.CREATED,
                            Instant.now()
                    ),
                    new OrderEvent(
                            "ORD-2026-003",
                            "CUST-9999",
                            CustomerType.VIP,
                            new BigDecimal("3500.00"),
                            List.of(
                                    new OrderItem("PROD-4", "Màn hình 4K 144Hz", 1, new BigDecimal("3500.00"))
                            ),
                            OrderStatus.PENDING_PAYMENT,
                            Instant.now()
                    )
            );

            for (OrderEvent order : sampleOrders) {
                // Key của message là OrderId để đảm bảo toàn bộ sự kiện của đơn này về sau luôn cùng Partition!
                String key = order.orderId();

                ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(TOPIC_NAME, key, order);

                log.info("--> [Main Thread] Đang gửi đơn hàng: ID={} | Khách={} | Tổng tiền={}",
                        order.orderId(), order.customerId(), order.totalAmount());

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            log.info("<-- [Thành công] Đơn '{}' đã lưu vào Partition={} | Offset={}",
                                    key, metadata.partition(), metadata.offset());
                        } else {
                            log.error("xxx [Thất bại] Lỗi khi gửi đơn '{}': ", key, exception);
                        }
                    }
                });
            }

            producer.flush();

        } catch (Exception e) {
            log.error("Lỗi vận hành OrderProducer: ", e);
        }

        log.info("OrderProducer đã hoàn tất nhiệm vụ!");
    }
}
