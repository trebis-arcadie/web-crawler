package org.trebis.example;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.trebis.example.header.Header;
import org.trebis.example.proxy.Proxy;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class WebCrawler implements Runnable {

    private Thread thread;
    private final Integer id;
    private final Worker worker;

    private int counter;

    public WebCrawler(int id, Worker worker) {
        this.id = id;
        this.worker = worker;
        counter = worker.getCounter().incrementAndGet();
    }

    @Override
    public void run() {
        try {
            Thread.sleep(worker.getThreadSleepOnStartMs());
            System.out.println(logHeader.get() + String.format("WebCrawler : start : %s %s %s %s",
                    id, worker.getLinks().size(), worker.getVisitedLinks().size(), counter));
            getNextLink().ifPresent(this::parseLink);
        } catch (Exception e) {
            System.out.println(logHeader.get() + String.format("WebCrawler : error : %s %s",
                    id, e.getMessage()));
        } finally {
            int currentCounter = worker.getCounter().decrementAndGet();
            System.out.println(logHeader.get() + String.format("WebCrawler : stop : %s %s %s %s",
                    id, worker.getLinks().size(), worker.getVisitedLinks().size(), currentCounter));
            if (currentCounter == 0) {
                worker.stop();
            }
        }
    }

    private Supplier<String> logHeader = () -> Thread.currentThread().getId() + " : ";

    private Optional<Map.Entry<String, Integer>> getNextLink() {
        worker.getLock().lock();
        try {
            if (worker.getLinks().size() + worker.getVisitedLinks().size() < worker.getMaxCount()) {
                Optional<Map.Entry<String, Integer>> nextLinkEntryOpt = worker.getLinks().entrySet().stream()
                        .findFirst();
                nextLinkEntryOpt
                        .filter(nextLinkEntry -> {
                            String link = nextLinkEntry.getKey();
                            boolean filter = !worker.getVisitedLinks().containsKey(link);
                            if (filter) {
                                System.out.println(logHeader.get() + String.format("Thread %s : getNextLink : link %s NEW", id, link));
                            } else {
                                System.out.println(logHeader.get() + String.format("Thread %s : getNextLink : ignored link %s EXISTED", id, link));
                            }
                            return filter;
                        })
                        .filter(nextLinkEntry -> {
                            String link = nextLinkEntry.getKey();
                            Integer depth = nextLinkEntry.getValue();
                            boolean filter = depth <= worker.getMaxDepth();
                            if (filter) {
                                System.out.println(logHeader.get() + String.format("Thread %s : getNextLink : depth %s : %s", id, link, depth));
                            } else {
                                System.out.println(logHeader.get() + String.format("Thread %s : getNextLink : ignored link %s MAX DEPTH", id, link));
                            }
                            return filter;
                        })
                        .ifPresent(nextLinkEntry -> {
                            String link = nextLinkEntry.getKey();
                            Integer depth = nextLinkEntry.getValue();
                            worker.getLinks().remove(link);
                            worker.getVisitedLinks().put(link, depth);
                            System.out.println(logHeader.get() + String.format("Thread %s : getNextLink : move to visited %s : %s",
                                    id, worker.getLinks().size(), worker.getVisitedLinks().size()));
                        });
                return nextLinkEntryOpt;
            } else {
                System.out.println(logHeader.get() + String.format("Thread %s : getNextLink : ignored link : MAX COUNT", id));
                return Optional.empty();
            }
        } finally {
            worker.getLock().unlock();
        }
    }

    private void parseLink(Map.Entry<String, Integer> linkEntry) {
        String link = linkEntry.getKey();
        Integer depth = linkEntry.getValue();
        try {
            Connection con = Jsoup.connect(link);

            // proxy
            if (worker.isUseProxy()) {
                Optional<Proxy> proxyOpt = worker.nextProxy(id);
                if (proxyOpt.isPresent()) {
                    Proxy proxy = proxyOpt.get();
                    System.out.println(logHeader.get() + String.format("Thread %s : parseLink : proxy : %s", id, proxy));
                    con = con
                            .proxy(proxy.getIp(), proxy.getPort());
                }
            }

            // header
            if (worker.isUseHeader()) {
                Optional<Header> headerOpt = worker.nextHeader(id);
                if (headerOpt.isPresent()) {
                    Header header = headerOpt.get();
                    System.out.println(logHeader.get() + String.format("Thread %s : parseLink : header : %s", id, header));
                    con = con
                            .userAgent(header.getUserAgent())
                            .header("Accept-Language", header.getAcceptLanguage())
                            .header("Accept-Encoding", header.getAcceptEncoding())
                            .header("Accept", header.getAccept())
                            .header("Referer", header.getReferer());
                }
            }

            Document doc = con.get();

            if (con.response().statusCode() == 200) {
                String title = doc.title();
                System.out.println(logHeader.get() + String.format("Thread %s : parseLink : visited link %s %s", id, link, title));

                Elements elements = doc.select("a[href]");
                System.out.println(logHeader.get() + String.format("Thread %s : parseLink : elements %s %s", id, link, elements.size()));
                elements.stream()
                        .map(element -> element.absUrl("href"))
                        .map(String::trim)
                        .peek(newLink -> System.out.println(logHeader.get() + String.format("Thread %s : parseLink : link %s", id, newLink)))
                        .filter(newLink -> !newLink.contains("#"))
                        .filter(newLink -> !newLink.contains("javascript"))
                        .forEach(newLink -> {
                            worker.getLock().lock();
                            try {
                                if (!worker.getVisitedLinks().containsKey(newLink)
                                        && !worker.getLinks().containsKey(newLink)) {
                                    if (worker.getLinks().size() + worker.getVisitedLinks().size() < worker.getMaxCount()) {
                                        int level = linkEntry.getValue();
                                        level++;
                                        if (level < worker.getMaxDepth()) {
                                            System.out.println(logHeader.get() + String.format("Thread %s : parseLink : filtered link %s : %s NEW", id, newLink, level));
                                            worker.getLinks().put(newLink, level);

                                            int newId = worker.getIds().incrementAndGet();
                                            System.out.println(logHeader.get() + String.format("Thread %s : parseLink : crawler %s : %s NEW", id, newLink, newId));

                                            worker.getExecutorService().submit(new WebCrawler(newId, worker));
                                        }
                                    } else {
                                        System.out.println(logHeader.get() + String.format("Thread %s : parseLink : ignored link %s MAX COUNT", id));
                                    }
                                } else {
                                    System.out.println(logHeader.get() + String.format("Thread %s : parseLink : ignored link %s EXISTED", id));
                                }
                            } finally {
                                worker.getLock().unlock();
                            }
                        });

                Elements productTitle = doc.select("span#productTitle");
                // TODO

                worker.getParsedCounter().incrementAndGet();
            }
        } catch (Exception e) {
            System.out.println(logHeader.get() + String.format("Thread %s : parseLink : error : %s", id, e.getMessage()));
            worker.getVisitedLinks().put(link, -1);
            worker.getErrorCounter().incrementAndGet();
        }
    }
}
