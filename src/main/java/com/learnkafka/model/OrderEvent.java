package com.learnkafka.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * =========================================================================
 * MODEL ĐƠN HÀNG THỰC TẾ (JAVA 21 RECORD)
 * =========================================================================
 * Java 21 Record:
 * - Bất biến (Immutable), tự động sinh constructor, getters, equals, hashCode, toString.
 * - Cực kỳ phù hợp cho Event-Driven Architecture / Kafka Message.
 */
public record OrderEvent(
        String orderId,
        String customerId,
        CustomerType customerType,
        BigDecimal totalAmount,
        List<OrderItem> items,
        OrderStatus status,
        Instant createdAt
) {
    public enum CustomerType {
        VIP, STANDARD
    }

    public enum OrderStatus {
        CREATED, PENDING_PAYMENT, COMPLETED, CANCELLED
    }

    public record OrderItem(
            String productId,
            String productName,
            int quantity,
            BigDecimal price
    ) {}
}
