package org.trebis.example;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("=========================");
        System.out.println("START");
        Worker worker = new Worker();
        worker.start();
        String rootLink = worker.getRootLink();
        int startDepth = 0;;
        worker.getLinks().put(rootLink, startDepth);
        int id = worker.getIds().incrementAndGet();
        worker.getExecutorService().submit(new WebCrawler(id, worker));
        worker.getDoneSignal().await();
        System.out.println("FINISH");
        System.out.println("=========================");

        System.out.println("remain links = " + worker.getLinks().size());
        System.out.println("visited links = " + worker.getVisitedLinks().size());
        System.out.println("parsed links = " + worker.getParsedCounter());
        System.out.println("errors links = " + worker.getErrorCounter());
        System.out.println("running threads = " + worker.getIds().get());
    }
}