package com.wt.labrador.test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author 一贫
 * @date 2021/9/26
 */
public class ThreadPoolTest {

    private static ThreadLocal<Integer> tl = ThreadLocal.withInitial(() -> {
        return 1;
    });

    public static void main(String[] args) throws Exception {
        MyThreadPoolExecutor pool = new MyThreadPoolExecutor(3, 5, 3L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

        while (true) {
            pool.submit(() -> {
                System.out.println("before:" + tl.get());
                tl.set(tl.get() + 1);
                System.out.println("after:" + tl.get());
            });
            Thread.sleep(1000L);
        }
    }

    static class MyThreadPoolExecutor extends ThreadPoolExecutor {

        public MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);

        }

        public void afterExecute(Runnable r, Throwable t) {
            tl.remove();
        }
    }
}
