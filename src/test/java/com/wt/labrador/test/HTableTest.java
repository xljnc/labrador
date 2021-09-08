package com.wt.labrador.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author qiyu
 * @date 2021/9/8
 */
public class HTableTest {
    public static void main(String[] args) {
        Configuration configuration =HBaseConfiguration.create();
        configuration.set("","");
    }
}
