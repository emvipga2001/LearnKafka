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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * =========================================================================================
 * BÀI HỌC: MÔ HÌNH ĐA LUỒNG NÂNG CAO (WORKER THREAD POOL & BẢO ĐẢM THỨ TỰ THEO KEY)
 * =========================================================================================
 * BỘ KHUNG SƯỜN (CODE SKELETON)
 * 
 * Mục tiêu:
 * 1. Dùng 1 luồng duy nhất để poll dữ liệu từ Kafka (tuân thủ Single-Thread của KafkaConsumer).
 * 2. Điều phối công việc sang ThreadPool gồm NUM_WORKERS luồng xử lý song song.
 * 3. BẢO ĐẢM THỨ TỰ: Dữ liệu có cùng Key (cùng khách hàng) luôn đi vào cùng 1 Worker Thread.
 */
public class WorkerPoolConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkerPoolConsumer.class);
    private static final String TOPIC_NAME = "order-events";
    private static final String GROUP_ID = "worker-pool-group";

    // Số lượng Worker xử lý độc lập trong ThreadPool
    private static final int NUM_WORKERS = 3;

    // Mỗi Worker có 1 hàng đợi (BlockingQueue) riêng để đảm bảo thứ tự tuần tự của nó
    @SuppressWarnings("unchecked")
    private static final BlockingQueue<ConsumerRecord<String, String>>[] workerQueues = new BlockingQueue[NUM_WORKERS];
    
    // Thread pool quản lý các worker
    private static final ExecutorService workerPool = Executors.newFixedThreadPool(NUM_WORKERS);
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        log.info("=== [KHỞI ĐỘNG WORKER POOL CONSUMER KIẾN TRÚC CAO CẤP] ===");

        // -----------------------------------------------------------------
        // PHẦN 1: KHỞI TẠO CÁC HÀNG ĐỢI VÀ KHỞI ĐỘNG WORKER THREADS
        // (Thầy đã viết sẵn 100%)
        // -----------------------------------------------------------------
        for (int i = 0; i < NUM_WORKERS; i++) {
            final int workerId = i;
            workerQueues[workerId] = new LinkedBlockingQueue<>(500); // Mỗi worker chứa tối đa 500 tasks trong RAM

            // Khởi chạy Worker Thread thứ i
            workerPool.submit(() -> {
                log.info("🚀 [WORKER #{}] Đã sẵn sàng nhận việc!", workerId);
                while (running.get() || !workerQueues[workerId].isEmpty()) {
                    try {
                        // Rút message từ hàng đợi của chính mình (chờ tối đa 500ms)
                        ConsumerRecord<String, String> record = workerQueues[workerId].poll(500, TimeUnit.MILLISECONDS);
                        if (record != null) {
                            // Giả lập thời gian xử lý nghiệp vụ nặng (gọi API, DB)
                            processRecord(workerId, record);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                log.info("🏁 [WORKER #{}] Đã dừng hoàn toàn.", workerId);
            });
        }

        // -----------------------------------------------------------------
        // PHẦN 2: CẤU HÌNH KAFKA CONSUMER CHO LUỒNG CHÍNH (POLL THREAD)
        // -----------------------------------------------------------------
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // Trong bài này tập trung vào kiến trúc phân luồng

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        // SHUTDOWN HOOK: Dừng êm đẹp
        final Thread pollThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.warn("Nhận tín hiệu dừng! Đang dừng luồng Poll và dọn dẹp Worker Pool...");
            running.set(false);
            consumer.wakeup();
            try {
                pollThread.join();
                workerPool.shutdown();
                if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    workerPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Lỗi khi shutdown: ", e);
            }
        }));

        // -----------------------------------------------------------------
        // PHẦN 3: VÒNG LẶP POLL VÀ ĐIỀU PHỐI (DISPATCH) THEO KEY
        // -----------------------------------------------------------------
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    
                    // 🎯 THỬ THÁCH DÀNH CHO BẠN (CORE LOGIC):
                    // Làm thế nào để chọn đúng workerIndex (từ 0 đến NUM_WORKERS - 1)
                    // sao cho cùng Key sẽ luôn rơi vào cùng 1 Worker?
                    //
                    // TODO: Hãy tính toán workerIndex dựa vào record.key():
                    // 1. Nếu record.key() != null: Dùng thuật toán băm Math.abs(record.key().hashCode()) % NUM_WORKERS
                    // 2. Nếu record.key() == null: Chọn worker mặc định 0 (hoặc round-robin)
                    // 3. Đẩy record vào hàng đợi của worker đó: workerQueues[workerIndex].put(record);
                    //
                    // >>> BẠN HÃY TỰ TAY VIẾT ĐOẠN ĐIỀU PHỐI DƯỚI ĐÂY: <<<

                    // Gõ code của bạn ở đây...
                    int workerIndex = record.key() != null ? Math.abs(record.key().hashCode()) % NUM_WORKERS : 0;
                    workerQueues[workerIndex].put(record);
                }
            }
        } catch (WakeupException e) {
            log.info("Luồng poll nhận tín hiệu Wakeup.");
        } catch (Exception e) {
            log.error("Lỗi luồng poll: ", e);
        } finally {
            consumer.close();
            log.info("=== KAFKA CONSUMER ĐÃ ĐÓNG ===");
        }
    }

    // Số lần thử lại tối đa trước khi đưa vào hàng đợi thư chết (DLQ)
    private static final int MAX_RETRIES = 3;

    /**
     * Phương thức xử lý có bảo vệ bằng RETRY và DLQ
     */
    private static void processRecord(int workerId, ConsumerRecord<String, String> record) {
        // 🎯 THỬ THÁCH 2 DÀNH CHO BẠN: CHIẾN LƯỢC XỬ LÝ LỖI (RETRY & DLQ)
        // Khi gọi executeBusinessLogic(record), nếu gặp ngoại lệ:
        // 1. Thử lại tối đa MAX_RETRIES lần.
        // 2. Mỗi lần thử lại thất bại, cho Thread ngủ một chút (Backoff: ví dụ 300ms * lần_thử).
        // 3. Nếu thử hết MAX_RETRIES lần mà vẫn lỗi (Poison Pill):
        //    Gọi hàm sendToDeadLetterQueue(workerId, record, exception) để cô lập lỗi,
        //    cho phép worker giải phóng và tiếp tục xử lý các đơn hàng phía sau!
        //
        // >>> BẠN HÃY TỰ TAY VIẾT LOGIC RETRY VÀ DLQ DƯỚI ĐÂY: <<<

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                // 1. Thực thi nghiệp vụ ngay (không sleep ở lần đầu tiên để tối ưu tốc độ)
                executeBusinessLogic(record);
                log.info("✅ [Worker #{}] Xử lý thành công đơn hàng: Key='{}' | Offset={}",
                        workerId, record.key(), record.offset());
                return; // Thành công thì thoát ngay lập tức!
            } catch (Exception e) {
                // 2. Kiểm tra xem đã hết số lượt thử lại chưa
                if (attempt == MAX_RETRIES) {
                    // Đã thử hết số lần mà vẫn lỗi -> Đây là Poison Pill!
                    log.error("❌ [Worker #{}] Đã thử {}/{} lần nhưng vẫn thất bại! Đẩy sang DLQ cho Offset {}.",
                            workerId, attempt, MAX_RETRIES, record.offset());

                    // Đẩy sang DLQ để cách ly lỗi
                    sendToDeadLetterQueue(workerId, record, e);

                    // Thoát êm thắm để Worker tiếp tục nhận đơn hàng mới từ Queue!
                    return;
                }

                // 3. Nếu còn lượt thử: Log cảnh báo và thực hiện Backoff (ngủ tăng dần theo số lần thử)
                log.warn("⚠️ [Worker #{}] Thử lần {}/{} thất bại do: {}. Sẽ thử lại sau {}ms...",
                        workerId, attempt, MAX_RETRIES, e.getMessage(), 300 * attempt);
                try {
                    Thread.sleep(300L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                // [LƯU Ý QUAN TRỌNG]: Tuyệt đối KHÔNG throw Exception ở đây,
                // vì throw sẽ phá vỡ vòng lặp và không bao giờ chạy được lượt thử tiếp theo!
            }
        }
    }

    /**
     * Giả lập logic nghiệp vụ thực tế (Ghi DB, Gọi Cổng thanh toán)
     * Có thể ném lỗi nếu gặp tin nhắn rác hoặc "thuốc độc" (Poison Pill)
     */
    private static void executeBusinessLogic(ConsumerRecord<String, String> record) throws Exception {
        // Nếu Value chứa từ khóa "POISON_PILL", giả lập lỗi nghiêm trọng không thể xử lý
        if (record.value() != null && record.value().contains("POISON_PILL")) {
            throw new IllegalArgumentException("💥 Lỗi dữ liệu độc hại (Poison Pill): Định dạng không hợp lệ!");
        }

        // Giả lập xử lý thành công tốn 200ms
        Thread.sleep(200);
    }

    /**
     * Giả lập gửi tin nhắn lỗi vào Dead Letter Topic (DLQ)
     */
    private static void sendToDeadLetterQueue(int workerId, ConsumerRecord<String, String> record, Exception e) {
        log.error("☠️ [DLQ ROUTING - Worker #{}] Đã gửi vào Topic 'order-events-dlq'! Key='{}' | Offset={} | Lỗi: {}",
                workerId, record.key(), record.offset(), e.getMessage());
    }
}
