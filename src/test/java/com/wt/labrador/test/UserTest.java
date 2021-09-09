package com.wt.labrador.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author qiyu
 * @date 2021/9/8
 */
@Slf4j
public class UserTest {
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin.available(config);
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();
        TableName userTableName = TableName.valueOf("users");
        if (!admin.tableExists(userTableName)) {
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(userTableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"));
            admin.createTable(builder.build());
        }
        Table user = connection.getTable(userTableName);
        Put put = new Put(Bytes.toBytes("qiyu"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("qiyu"));
        user.put(put);
    }
}
