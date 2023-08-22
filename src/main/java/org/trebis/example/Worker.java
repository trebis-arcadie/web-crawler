package org.trebis.example;

import lombok.Getter;
import org.trebis.example.header.Header;
import org.trebis.example.header.Headers;
import org.trebis.example.proxy.Proxies;
import org.trebis.example.proxy.Proxy;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.*;
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
    private boolean useProxy;
    private boolean useHeader;
    private String rootLink;
    private Headers headers;
    private final AtomicInteger headerCounter;
    private Proxies proxies;
    private final AtomicInteger proxyCounter;

    public Worker() {
        doneSignal = new CountDownLatch(1);
        counter = new AtomicInteger(0);
        ids = new AtomicInteger(0);
        parsedCounter = new AtomicInteger(0);
        errorCounter = new AtomicInteger(0);
        lock = new ReentrantLock();
        links = new HashMap<>();
        visitedLinks = new HashMap<>();
        headerCounter = new AtomicInteger(0);
        proxyCounter = new AtomicInteger(0);
    }

    public void start() throws Exception {
        initProperties();
        if (useProxy) {
            initProxies();
        }
        if (useHeader) {
            initHeaders();
        }
        executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    private void initProperties() throws Exception {
        Properties props = new Properties();
        try (InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            props.load(inputStream);
            maxDepth = Integer.parseInt(props.getProperty("maxDepth", "5"));
            maxCount = Integer.parseInt(props.getProperty("maxCount", "1000"));
            threadPoolSize = Integer.parseInt(props.getProperty("threadPoolSize", "50"));
            threadSleepOnStartMs = Integer.parseInt(props.getProperty("threadSleepOnStartMs", "0"));
            useProxy = Boolean.parseBoolean(props.getProperty("useProxy", "false"));
            useHeader = Boolean.parseBoolean(props.getProperty("useHeader", "false"));
            rootLink = props.getProperty("rootLink", "http//amazon.com");
            System.out.println("load props : " + props);
        }
    }

    private void initHeaders() throws Exception {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new Constructor(Headers.class, options));
        try (InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("headers.yml")) {
            headers = yaml.load(inputStream);
            System.out.println(headers);
        }
    }

    public Optional<Header> nextHeader(int threadId) {
        int count = headerCounter.getAndIncrement();
        int size = headers.getHeaders().size();
        if (size > 0) {
            int i = count % size;
            System.out.println(String.format("Thread %s : nextHeader : %s %s", threadId, size, i));
            return Optional.of(headers.getHeaders().get(i));
        }
        return Optional.empty();
    }

    private void initProxies() throws Exception {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new Constructor(Proxies.class, options));
        try (InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("proxies.yml")) {
            proxies = yaml.load(inputStream);
            System.out.println(proxies);
        }
    }

    public Optional<Proxy> nextProxy(int threadId) {
        int count = proxyCounter.getAndIncrement();
        int size = proxies.getProxies().size();
        if (size > 0) {
            int i = count % size;
            System.out.println(String.format("Thread %s : nextProxy : %s %s", threadId, size, i));
            return Optional.of(proxies.getProxies().get(i));
        }
        return Optional.empty();
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
