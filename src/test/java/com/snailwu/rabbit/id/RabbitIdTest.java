package com.snailwu.rabbit.id;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author WuQinglong
 */
public class RabbitIdTest {
    private static final Logger log = LoggerFactory.getLogger(RabbitIdTest.class);

    @Test
    public void nextId() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);

        // 定时输出 QPS
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            log.info("当前秒Id生成数:{}万", count.get() / 10000);
            count.getAndSet(0);
        }, 0, 1, TimeUnit.SECONDS);

        // 循环生成
        RabbitId snowflake = new RabbitId(1, 1);
        for (int i = 0; i < 100; i++) {
            new Thread(() -> {
                while (true) {
                    long id = snowflake.nextId();
                    count.incrementAndGet();
                }
            }).start();
        }

        TimeUnit.HOURS.sleep(1);
    }

}