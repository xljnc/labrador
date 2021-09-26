package com.wt.labrador.test;

import java.util.Arrays;
import java.util.List;

/**
 * @author 一贫
 * @date 2021/9/26
 */
public class ThreadLocalTest {
    public static void main(String[] args) {
        ThreadLocal<List<Integer>> tl = ThreadLocal.withInitial(() -> {
            return Arrays.asList(1, 2, 3);
        });
        System.out.println(tl.get() == null);
        System.out.println(tl.get());
    }
}
