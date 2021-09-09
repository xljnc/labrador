package com.wt.labrador.util;

import com.wt.labrador.exception.LabradorException;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * @author 一贫
 * @date 2021/9/9
 */
@Component
@Setter
@ConfigurationProperties(prefix = "hbase")
@Slf4j
public class HBaseUtil {

    private ZookeeperProperty zookeeper;

    private Configuration config;

    /**
     * Connection对象建议一个进程复用一个,是线程安全的
     * Admin,Table非线程安全，每个线程重新获取
     */
    private Connection connection;

    /**
     * 创建命名空间
     *
     * @param namespace 命名空间,为空时默认使用default
     * @return void
     */
    public void createNamespace(String namespace) {
        try {
            Admin admin = connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
            log.info("创建namespace {} 成功.", namespace);
        } catch (Exception e) {
            String msg = String.format("创建namespace %s 失败.", namespace);
            log.error(msg, namespace, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 创建表
     *
     * @param tableName      表名
     * @param columnFamilies 列族，至少一个
     * @param namespace      命名空间,为空时默认使用default
     * @return void
     */
    public void createTable(String tableName, String[] columnFamilies, String namespace) {
        if (columnFamilies == null || columnFamilies.length == 0)
            throw new RuntimeException("创建表至少要指定1个列族.");
        if (StringUtils.isNotBlank(namespace))
            tableName = namespace + ":" + tableName;
        TableName tabName = TableName.valueOf(tableName);
        try {
            Admin admin = connection.getAdmin();
            if (admin.tableExists(tabName))
                throw new LabradorException(String.format("表 %s 已经存在", tableName));
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tabName);
            for (String columnFamily : columnFamilies) {
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
            }
            admin.createTable(builder.build());
            log.info("创建table {} 成功.", tableName);
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("创建namespace %s 失败.", namespace);
            log.error(msg, namespace, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 创建表,使用默认命名空间default
     *
     * @param tableName      表名
     * @param columnFamilies 列族，至少一个
     * @return void
     */
    public void createTable(String tableName, String[] columnFamilies) {
        createTable(tableName, columnFamilies, null);
    }

    @PostConstruct
    public void init() throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeper.quorum);
        HBaseAdmin.available(config);
        connection = ConnectionFactory.createConnection(config);
    }

    @Setter
    private static class ZookeeperProperty {
        private String quorum;
    }

}
