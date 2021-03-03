package me.arnu.lru;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class LRUTest {

    public static void main(String[] args) throws IOException {
        Map<String, Integer> dataSource = new HashMap<String, Integer>();
        for (int i = 0; i < 8000; i++) {
            dataSource.put("a" + i, i);
        }

        LRUCacheManager<String, Integer> cacheManager = new LRUCacheManager<String, Integer>(10000,(k)->dataSource);
        for (int i = 0; i < 10; i++) {
            new MyThread(cacheManager, "thread:" + i).start();
        }

        System.in.read();
        cacheManager.printSize();
    }

    static class MyThread extends Thread {

        private LRUCacheManager<String, Integer> cacheManager;

        public MyThread(LRUCacheManager<String, Integer> cacheManager, String name) {
            super(name);
            this.cacheManager = cacheManager;
        }

        Random random = new Random();

        @Override
        public void run() {
            int i = 0;
            try {
                while (i < 10000) {
                    cacheManager.GetValue("a" + random.nextInt(6000));
                    i++;
                }
                System.out.println(Thread.currentThread().getName() + "done");
            } catch (
                    Exception e) {
                System.out.println(Thread.currentThread().getName() + "error, " + i);
                e.printStackTrace();
            }
        }

    }
}