package com.snailwu.rabbit.id;

import java.lang.management.ManagementFactory;
import java.net.NetworkInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SnowFlake的结构如下(每部分用-分开):<br>
 * <br>
 * 0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000 <br>
 * <br>
 * 1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0<br>
 * <br>
 * 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截)
 * 得到的值），这里的的开始时间截，一般是我们的id生成器开始使用的时间，由我们程序来指定的（如下START_TIME属性）。41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69<br>
 * <br>
 * 10位的数据机器位，可以部署在1024个节点，包括5位dataCenterId和5位workerId<br>
 * <br>
 * 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号，1秒中理论可以生成 999*4096=4091904 个id<br>
 * <br>
 * 加起来刚好64位，为一个Long型。<br>
 * SnowFlake的优点是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞(由数据中心ID和机器ID作区分)，并且效率较高，经测试，SnowFlake每秒能够产生40万ID左右。
 * <p>
 * @author WuQinglong
 */
public class RabbitId {
    private static final Logger log = LoggerFactory.getLogger(NetUtil.class);

    /**
     * 时间起始标记点，作为基准，一般取系统的最近时间（一旦确定不能变动）
     */
    private static final long BASE_TIMESTAMP = 1655613369856L;

    /**
     * dataCenterId 的位数 5位
     */
    private static final long DATA_CENTER_ID_BITS = 5L;

    /**
     * workId 的位数 5位
     */
    private static final long WORKER_ID_BITS = 5L;

    /**
     * 相同毫秒序列号的位数 12位，可以统一毫秒内生成 4096 个id
     */
    private static final long SEQUENCE_BITS = 12L;

    /**
     * dataCenterId 的最大值
     */
    private static final long MAX_DATA_CENTER_ID = ~(-1L << DATA_CENTER_ID_BITS);

    /**
     * workId 的最大值
     */
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    /**
     * 序列号的最大值
     */
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);

    // 移位相关
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATA_CENTER_ID_BITS;

    /**
     * 所属机房id
     */
    private final long datacenterId;

    /**
     * 所属机器id
     */
    private final long workerId;

    /**
     * 并发控制序列
     */
    private long sequence = 0L;

    /**
     * 上次生产 ID 时间戳
     */
    private long lastTimestamp = -1L;

    public RabbitId() {
        this.datacenterId = dataCenterId();
        this.workerId = workerId(dataCenterId());
        log.info("datacenterId={}", this.datacenterId);
        log.info("workerId={}", this.workerId);
    }

    public RabbitId(long datacenterId, long workerId) {
        this.datacenterId = datacenterId;
        this.workerId = workerId;
    }

    /**
     * 获取数据中心的Id
     * 使用 MAC 网卡地址的后两个16进制参与计算
     */
    private long dataCenterId() {
        long id = 0L;
        try {
            NetworkInterface network = NetworkInterface.getByInetAddress(NetUtil.getLocalAddress());
            if (network == null) {
                return 1L;
            }

            byte[] mac = network.getHardwareAddress();
            if (mac == null) {
                return 1L;
            }

            id = (0x000000FF & mac[mac.length - 2]) | (0x0000FF00 & (mac[mac.length - 1] << 8));
            id = id % (MAX_DATA_CENTER_ID + 1);
        } catch (Exception e) {
            log.warn("getDatacenterId: " + e.getMessage());
        }
        return id;
    }

    /**
     * 基于 MAC + PID 的 hashcode 获取16个低位
     */
    private long workerId(long datacenterId) {
        StringBuilder builder = new StringBuilder();
        builder.append(datacenterId);
        String name = ManagementFactory.getRuntimeMXBean().getName();
        if (name != null && name.length() > 0) {
            // GET jvmPid
            builder.append(name.split("@")[0]);
        }

        // MAC + PID 的 hashcode 获取16个低位
        return (builder.toString().hashCode() & 0xffff) % (MAX_WORKER_ID + 1);
    }

    /**
     * 获取 id
     */
    public synchronized long nextId() {
        long millis = System.currentTimeMillis();

        if (millis == lastTimestamp) {
            // 相同毫秒内，序列号自增
            sequence = sequence + 1;
            if (sequence >= MAX_SEQUENCE) {
                // 同一毫秒的序列数已经达到最大，等待下一毫秒
                millis = waitToNextMillis(lastTimestamp);
                // 等待下一毫秒直接把自增序列号重置为0
                sequence = 0L;
            }
        } else {
            // 不同毫秒内，序列号一直是 0，也可以扩展为随机数
            sequence = 0L;
        }

        lastTimestamp = millis;

        // 时间戳部分 | 数据中心部分 | 机器标识部分 | 序列号部分
        return ((millis - BASE_TIMESTAMP) << TIMESTAMP_LEFT_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    /**
     * 等到下一毫秒
     */
    private long waitToNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

}
