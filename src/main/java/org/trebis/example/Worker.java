package org.trebis.example;

import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Getter
public class Worker {

    private final CountDownLatch doneSignal;
    private final AtomicInteger counter;
    private final AtomicInteger ids;
    private final AtomicInteger parsedCounter;
    private final AtomicInteger errorCounter;
    private final ReentrantLock lock;
    private final Map<String, Integer> links;
    private final Map<String, Integer> visitedLinks;
    private ExecutorService executorService;
    private int maxDepth;
    private int maxCount;
    private int threadPoolSize;
    private int threadSleepOnStartMs;
    private String rootLink;

    public Worker() {
        doneSignal = new CountDownLatch(1);
        counter = new AtomicInteger(0);
        ids = new AtomicInteger(0);
        parsedCounter = new AtomicInteger(0);
        errorCounter = new AtomicInteger(0);
        lock = new ReentrantLock();
        links = new HashMap<>();
        visitedLinks = new HashMap<>();
    }

    public void start() throws Exception {
        initProperties();
        executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    private void initProperties() throws Exception {
        Properties props = new Properties();
        try (InputStream inputStream = Worker.class.getResourceAsStream("/application.properties")) {
            props.load(inputStream);
            maxDepth = Integer.parseInt(props.getProperty("maxDepth", "5"));
            maxCount = Integer.parseInt(props.getProperty("maxCount", "1000"));
            threadPoolSize = Integer.parseInt(props.getProperty("threadPoolSize", "50"));
            threadSleepOnStartMs = Integer.parseInt(props.getProperty("threadSleepOnStartMs", "0"));
            rootLink = props.getProperty("rootLink", "http//amazon.com");
            System.out.println("load props : " + props);
        }
    }

    public void stop() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            doneSignal.countDown();
        }
    }
}
