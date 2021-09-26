package com.wt.labrador.test;

/**
 * @author 一贫
 * @date 2021/9/18
 */
public class NullTest {
    public static void main(String[] args) {
        Long l1 = Long.valueOf(123);
        Long l2 = null;
        System.out.println(l1.equals(l2));
    }
}
