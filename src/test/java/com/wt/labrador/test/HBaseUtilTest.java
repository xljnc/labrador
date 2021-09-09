package com.wt.labrador.test;

import com.wt.labrador.util.HBaseUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author 一贫
 * @date 2021/9/9
 */
@SpringBootTest
public class HBaseUtilTest {

    @Autowired
    private HBaseUtil hBaseUtil;

    @Test
    public void testCreateTable() {
        hBaseUtil.createTable("users", new String[]{"info"}, "qiyu");
    }

    @Test
    public void testCreateNamespace() {
        hBaseUtil.createNamespace("qiyu");
    }
}
