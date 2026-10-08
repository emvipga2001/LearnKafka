package com.learnkafka.producer.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * =========================================================================
 * BÀI HỌC: CUSTOM PARTITIONER (MODULE 2 - BÀI 2)
 * =========================================================================
 * Nghiệp vụ thực tế: Phân luồng ưu tiên (VIP Isolation Pattern)
 * - Nếu Key bắt đầu bằng "VIP-": Luôn gán vào Partition 0 (Làn ưu tiên).
 * - Nếu Key là khách thường: Dùng thuật toán Murmur2 băm đều vào các Partition còn lại (Partition 1, 2...).
 */
public class VipOrderPartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(VipOrderPartitioner.class);

    // Cấu hình tên tiền tố VIP (có thể truyền động qua Properties)
    private String vipPrefix = "VIP-";

    /**
     * Hàm được gọi 1 lần duy nhất khi Producer khởi tạo.
     * Cho phép ta đọc các cấu hình tùy biến truyền từ Producer Properties vào.
     */
    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey("vip.prefix")) {
            this.vipPrefix = configs.get("vip.prefix").toString();
        }
        log.info("VipOrderPartitioner đã được khởi tạo với tiền tố VIP: '{}'", this.vipPrefix);
    }

    /**
     * HÀM CỐT LÕI (CORE METHOD): Quyết định message rơi vào partition số mấy.
     * 
     * @param topic      Tên topic đang gửi tới
     * @param key        Key dạng Object (thường là String)
     * @param keyBytes   Mảng byte của key sau khi đã qua Serializer
     * @param value      Value dạng Object
     * @param valueBytes Mảng byte của value sau khi đã qua Serializer
     * @param cluster    Thông tin siêu dữ liệu (Metadata) về toàn bộ cụm Kafka hiện tại
     * @return Số nguyên đại diện cho Partition đích (0, 1, 2...)
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                          Object value, byte[] valueBytes, Cluster cluster) {

        // Lấy danh sách tất cả các partition khả dụng của Topic này
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        int numPartitions = partitions.size();

        // Kiểm tra an toàn: Nếu topic chỉ có 1 partition thì bắt buộc trả về 0
        if (numPartitions <= 1) {
            return 0;
        }

        // BƯỚC 1: Xử lý trường hợp Key bị null (Người gửi quên truyền key)
        if (keyBytes == null || !(key instanceof String keyStr)) {
            // Khi không có key, ta chia đều vào các partition thông thường (từ 1 trở đi)
            return 1 + (Utils.toPositive(Utils.murmur2(valueBytes)) % (numPartitions - 1));
        }

        // BƯỚC 2: Kiểm tra nghiệp vụ VIP
        if (keyStr.startsWith(vipPrefix)) {
            // Khách hàng VIP: Luôn luôn đi vào Partition 0 (Làn thương gia)
            log.info("⭐ [VIP Priority] Đơn hàng VIP '{}' được cấp làn riêng -> Partition 0", keyStr);
            return 0;
        }

        // BƯỚC 3: Khách hàng thường (Standard)
        // Dùng thuật toán băm Murmur2 chuẩn của Kafka để chia đều vào các partition còn lại (từ 1 đến numPartitions - 1)
        int standardPartition = 1 + (Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - 1));
        log.info("👤 [Standard] Đơn hàng thường '{}' băm vào -> Partition {}", keyStr, standardPartition);
        return standardPartition;
    }

    /**
     * Được gọi khi Producer đóng kết nối (close).
     * Dùng để giải phóng tài nguyên mạng, database connection nếu có.
     */
    @Override
    public void close() {
        log.info("VipOrderPartitioner đã đóng thành công.");
    }
}
