package com.learnkafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * =========================================================================
 * GENERIC JSON SERIALIZER DÙNG CHUNG CHO MỌI JAVA OBJECT
 * =========================================================================
 * Biến bất kỳ Java Object nào (T) thành byte[] JSON thông qua Jackson ObjectMapper.
 */
public class JsonSerializer<T> implements Serializer<T> {

    private static final Logger log = LoggerFactory.getLogger(JsonSerializer.class);

    // ObjectMapper là thread-safe sau khi cấu hình, ta dùng 1 instance duy nhất
    private final ObjectMapper objectMapper;

    public JsonSerializer() {
        this.objectMapper = new ObjectMapper();
        // Đăng ký module hỗ trợ Java 8 / 21 Time (Instant, LocalDateTime)
        this.objectMapper.registerModule(new JavaTimeModule());
        // Không xuất timestamp dưới dạng số mili-giây, mà xuất dưới dạng chuỗi ISO-8601 chuẩn quốc tế
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Có thể đọc config nếu cần, để trống nếu không có nhu cầu
    }

    /**
     * HÀM CỐT LÕI: Biến Object data thành mảng byte[]
     */
    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null; // Trả về null để biểu thị Tombstone message (sẽ học ở Module 8)
        }

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            log.error("Lỗi khi serialize object sang JSON byte[] cho topic '{}': ", topic, e);
            throw new SerializationException("Không thể chuyển đổi object sang JSON byte[]: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        // Dọn dẹp tài nguyên nếu có
    }
}
