package com.learnkafka.producer;

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
 * BÀI HỌC: JAVA PRODUCER CHUYÊN SÂU (MODULE 2 - BÀI 1)
 * =========================================================================
 * Mục tiêu: 
 * 1. Hiểu kiến trúc 2 luồng: Main Thread vs Background Sender Thread.
 * 2. Nắm vững cơ chế gom batch trong RecordAccumulator (batch.size & linger.ms).
 * 3. Gửi tin nhắn bất đồng bộ kèm Callback chuẩn Production (non-blocking).
 */
public class BasicProducer {

    private static final Logger log = LoggerFactory.getLogger(BasicProducer.class);
    private static final String TOPIC_NAME = "order-events";

    public static void main(String[] args) {
        log.info("Khởi tạo cấu hình cho Kafka Producer...");

        // -----------------------------------------------------------------
        // BƯỚC 1: THIẾT LẬP THUỘC TÍNH (PRODUCER CONFIGURATIONS)
        // -----------------------------------------------------------------
        Properties props = new Properties();

        // 1. Địa chỉ kết nối tới cụm Kafka (Host và Port)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 2. Serializer: Kafka bên dưới tầng network CHỈ HIỂU MẢNG BYTE (byte[]).
        // Ta phải hướng dẫn Kafka cách chuyển đổi Java String thành byte[] để bắn qua mạng.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 3. Cơ chế xác nhận biên lai (Acks - Acknowledgements)
        // acks = "all" (hoặc "-1"): Đảm bảo Broker Leader và tất cả các replica trong ISR 
        // đã ghi dữ liệu vào log an toàn thì mới báo thành công (Độ tin cậy cao nhất!).
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 4. Số lần tự động thử lại khi gặp lỗi mạng tạm thời (RetriableException)
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); // Đợi 1s giữa các lần retry

        // 5. TỐI ƯU HIỆU NĂNG BATCHING (Xe buýt gom khách trong RAM)
        // batch.size: Dung lượng tối đa của một thùng chứa batch (16KB = 16384 bytes).
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // linger.ms: Thời gian tối đa Sender Thread chấp nhận chờ thêm message mới 
        // để nhét chung vào batch trước khi bấm nút gửi qua mạng (ở đây là chờ tối đa 20ms).
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        // -----------------------------------------------------------------
        // BƯỚC 2: KHỞI TẠO KAFKAPRODUCER & GỬI MESSAGE
        // -----------------------------------------------------------------
        // Dùng try-with-resources để đảm bảo Producer luôn được đóng (close) an toàn,
        // xả sạch (flush) mọi message còn sót trong bộ đệm RAM trước khi JVM tắt.
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            log.info("KafkaProducer đã kết nối thành công tới cluster!");

            // Ta sẽ gửi thử nghiệm 5 message đại diện cho 5 đơn hàng
            for (int i = 1; i <= 5; i++) {
                String key = "order-id-" + i;
                String value = "{\"orderId\": " + i + ", \"amount\": " + (i * 150) + ", \"status\": \"CREATED\"}";

                // Tạo đối tượng ProducerRecord gồm: Topic, Key, Value
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);

                log.info("--> [Main Thread] Đẩy record vào RecordAccumulator: key={}", key);

                // ---------------------------------------------------------
                // GỬI BẤT ĐỒNG BỘ KÈM CALLBACK (ASYNCHRONOUS SEND)
                // ---------------------------------------------------------
                // Hàm send() không chặn luồng (Non-blocking). Nó chỉ ném record vào bộ nhớ RAM
                // rồi trả về ngay lập tức. Sender Thread chạy ngầm sẽ chịu trách nhiệm gửi.
                // Khi Broker phản hồi biên lai (Ack), hàm onCompletion() sẽ được kích hoạt!
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            // Thành công: Broker đã ghi vào log và trả về tọa độ vật lý!
                            log.info("<-- [Callback Success] Đã lưu vào Topic: {} | Partition: {} | Offset: {} | Timestamp: {}",
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset(),
                                    metadata.timestamp());
                        } else {
                            // Thất bại: Gặp lỗi không thể phục hồi
                            log.error("xxx [Callback Error] Gửi message thất bại: {}", exception.getMessage(), exception);
                        }
                    }
                });
            }

            // Xả toàn bộ bộ đệm trong RAM (bắt buộc Sender Thread gửi ngay không đợi linger.ms)
            log.info("Gọi flush() để đảm bảo Sender Thread chuyển toàn bộ batch qua mạng...");
            producer.flush();

        } catch (Exception e) {
            log.error("Lỗi nghiêm trọng trong quá trình vận hành Producer: ", e);
        }

        log.info("Chương trình hoàn tất! Producer đã đóng kết nối.");
    }
}
