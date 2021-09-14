package com.wt.labrador.test;

import com.wt.labrador.util.HBaseUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

/**
 * @author 一贫
 * @date 2021/9/9
 */
@SpringBootTest
public class HBaseUtilTest {

    @Autowired
    private HBaseUtil hBaseUtil;

    @Test
    public void testCreateNamespace() {
        hBaseUtil.createNamespace("qiyu");
    }

    @Test
    public void testCreateTable() {
        hBaseUtil.createTable("users", new String[]{"info","work"}, "qiyu");
    }

    @Test
    public void testPut() {
        hBaseUtil.put("users", "qiyu", "info", "name", "qiyu", "qiyu");
        hBaseUtil.put("users", "qiyu", "info", "age", "12", "qiyu");
        hBaseUtil.put("users", "qiyu", "work", "company", "工地", "qiyu");
        hBaseUtil.put("users", "ada", "info", "name", "阿大", "qiyu");
        hBaseUtil.put("users", "ada", "info", "age", "18", "qiyu");
        hBaseUtil.put("users", "ada", "work", "company", "马路局", "qiyu");
    }

    @Test
    public void testGetColumn() {
        String result = hBaseUtil.getColumn("users", "qiyu", "info", "name", "qiyu");
        System.out.println(result);
    }

    @Test
    public void testGetColumnFamily() {
        Map<String, String> map = hBaseUtil.getColumnFamily("users", "qiyu", "info", "qiyu");
        System.out.println(map);
    }

    @Test
    public void testGetRow() {
        Map<String, Map<String, String>> map = hBaseUtil.getRow("users", "qiyu", "qiyu");
        System.out.println(map);
    }

    @Test
    public void testDeleteRow() {
        hBaseUtil.deleteRow("users", "qiyu", "qiyu");
    }

    @Test
    public void testDeleteColumnFamily() {
        hBaseUtil.deleteColumnFamily("users", "qiyu", new String[]{"info", "work"}, "qiyu");
    }

    @Test
    public void testDeleteColumn() {
        hBaseUtil.deleteColumn("users", "qiyu", "info", new String[]{"name", "age"}, "qiyu");
    }

    @Test
    public void testScan() {
        Map<String, Map<String, Map<String, String>>> result = hBaseUtil.scan("users", "ada", null, new String[]{"info", "work"}, "qiyu");
        System.out.println(result);
    }

    @Test
    public void testScanColumnFamily() {
        Map<String, Map<String, Map<String, String>>> result = hBaseUtil.scanColumnFamily("users", "ada", null, "info", new String[]{"name", "age"}, "qiyu");
        System.out.println(result);
    }
}
