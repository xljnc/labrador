package com.wt.labrador.util;

import com.wt.labrador.exception.LabradorException;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

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
            log.error(msg, e);
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
        tableName = buildTableNameWithNameSpace(tableName, namespace);
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
            String msg = String.format("创建表 %s 失败.", namespace);
            log.error(msg, e);
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

    /**
     * 保存数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param data
     * @param namespace
     * @return void
     */
    public void put(String tableName, String rowKey, String columnFamily, String column, String data, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("保存数据失败,table:%s,rowKey:%s,columnFamily:%s", tableName, rowKey, columnFamily);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 保存数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param data
     * @return void
     */
    public void put(String tableName, String rowKey, String columnFamily, String column, String data) {
        put(tableName, rowKey, columnFamily, column, data, null);
    }

    /**
     * 保存数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param keyValues,   K: column qualifier,V: data
     * @param namespace
     * @return void
     */
    public void put(String tableName, String rowKey, String columnFamily, Map<String, String> keyValues, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Put put = new Put(Bytes.toBytes(rowKey));
            byte[] rowKeyBytes = Bytes.toBytes(columnFamily);
            keyValues.forEach((k, v) -> {
                put.addColumn(rowKeyBytes, Bytes.toBytes(k), Bytes.toBytes(v));
            });
            table.put(put);
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("保存数据失败,table:%s,rowKey:%s,columnFamily:%s", tableName, rowKey, columnFamily);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 保存数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param keyValues,   K: column qualifier,V: data
     * @return void
     */
    public void put(String tableName, String rowKey, String columnFamily, Map<String, String> keyValues) {
        put(tableName, rowKey, columnFamily, keyValues, null);
    }

    /**
     * 获取某个列的数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param namespace
     * @return String
     */
    public String getColumn(String tableName, String rowKey, String columnFamily, String column, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            Result result = table.get(get);
            return Bytes.toString(result.value());
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("获取数据失败,table:%s,rowKey:%s,columnFamily:%s,column:%s", tableName, rowKey, columnFamily, column);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 获取某个列的数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @return String
     */
    public String getColumn(String tableName, String rowKey, String columnFamily, String column) {
        return getColumn(tableName, rowKey, columnFamily, column, null);
    }

    /**
     * 获取某个列族数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param namespace
     * @return Map<String, String>
     */
    public Map<String, String> getColumnFamily(String tableName, String rowKey, String columnFamily, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(columnFamily));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            Map<String, String> kv = new HashMap<>();
            if (CollectionUtils.isEmpty(cells))
                return kv;
            for (Cell cell : cells) {
                String column = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                kv.put(column, value);
            }
            return kv;
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("获取数据失败,table:%s,rowKey:%s,columnFamily:%s", tableName, rowKey, columnFamily);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 获取某个列族数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @return Map<String, String>
     */
    public Map<String, String> getColumnFamily(String tableName, String rowKey, String columnFamily) {
        return getColumnFamily(tableName, rowKey, columnFamily, null);
    }

    /**
     * 获取某行数据
     *
     * @param tableName
     * @param rowKey
     * @param namespace
     * @return Map<String, String>
     */
    public Map<String, Map<String, String>> getRow(String tableName, String rowKey, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            return resultToMap(result);
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("获取数据失败,table:%s,rowKey:%s", tableName, rowKey);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 获取某行数据
     *
     * @param tableName
     * @param rowKey
     * @return Map<String, String>
     */
    public Map<String, Map<String, String>> getRow(String tableName, String rowKey) {
        return getRow(tableName, rowKey, null);
    }

    /**
     * 删除某行数据
     *
     * @param tableName
     * @param rowKey
     * @param namespace
     * @return void
     */
    public void deleteRow(String tableName, String rowKey, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("删除数据失败,table:%s,rowKey:%s", tableName, rowKey);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }


    /**
     * 删除某行数据
     *
     * @param tableName
     * @param rowKey
     * @return void
     */
    public void deleteRow(String tableName, String rowKey) {
        deleteRow(tableName, rowKey, null);
    }

    /**
     * 删除某行某列族数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamilies
     * @param namespace
     * @return void
     */
    public void deleteColumnFamily(String tableName, String rowKey, String[] columnFamilies, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            for (String columnFamily : columnFamilies) {
                delete.addFamily(Bytes.toBytes(columnFamily));
            }
            table.delete(delete);
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("删除数据失败,table:%s,rowKey:%s", tableName, rowKey);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 删除某行某列族数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamilies
     * @return void
     */
    public void deleteColumnFamily(String tableName, String rowKey, String[] columnFamilies) {
        deleteColumnFamily(tableName, rowKey, columnFamilies, null);
    }

    /**
     * 删除某行某列族数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columns
     * @param namespace
     * @return void
     */
    public void deleteColumn(String tableName, String rowKey, String columnFamily, String[] columns, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            byte[] cfBytes = Bytes.toBytes(columnFamily);
            for (String column : columns) {
                delete.addColumn(cfBytes, Bytes.toBytes(column));
            }
            table.delete(delete);
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("删除数据失败,table:%s,rowKey:%s,columnFamily:%s", tableName, rowKey, columnFamily);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 删除某行某列族数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columns
     * @return void
     */
    public void deleteColumn(String tableName, String rowKey, String columnFamily, String[] columns) {
        deleteColumn(tableName, rowKey, columnFamily, columns, null);
    }

    /**
     * 扫描数据
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columnFamilies
     * @param namespace
     * @return void
     */
    public Map<String, Map<String, Map<String, String>>> scan(String tableName, String startRow, String stopRow, String[] columnFamilies, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Scan scan = new Scan();
            if (StringUtils.isNotBlank(startRow))
                scan.withStartRow(Bytes.toBytes(startRow));
            if (StringUtils.isNotBlank(stopRow))
                scan.withStopRow(Bytes.toBytes(stopRow));
            if (columnFamilies != null && columnFamilies.length > 0) {
                for (String columnFamily : columnFamilies) {
                    scan.addFamily(Bytes.toBytes(columnFamily));
                }
            }
            ResultScanner rs = table.getScanner(scan);
            Map<String, Map<String, Map<String, String>>> rows = new LinkedHashMap<>();
            for (Result result : rs) {
                Map<String, Map<String, String>> map = resultToMap(result);
                rows.put(Bytes.toString(result.getRow()), map);
            }
            return rows;
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("扫描数据失败,table:%s", tableName);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 扫描数据
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columnFamilies
     * @return void
     */
    public Map<String, Map<String, Map<String, String>>> scan(String tableName, String startRow, String stopRow, String[] columnFamilies) {
        return scan(tableName, startRow, stopRow, columnFamilies, null);
    }

    /**
     * 扫描某个列族数据
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columnFamily
     * @param columns
     * @param namespace
     * @return void
     */
    public Map<String, Map<String, Map<String, String>>> scanColumnFamily(String tableName, String startRow, String stopRow, String columnFamily, String[] columns, String namespace) {
        tableName = buildTableNameWithNameSpace(tableName, namespace);
        try {
            TableName tabName = TableName.valueOf(tableName);
            if (!connection.getAdmin().tableExists(tabName))
                throw new LabradorException(String.format("表 %s 不存在", tableName));
            Table table = connection.getTable(tabName);
            Scan scan = new Scan();
            if (StringUtils.isNotBlank(startRow))
                scan.withStartRow(Bytes.toBytes(startRow));
            if (StringUtils.isNotBlank(stopRow))
                scan.withStopRow(Bytes.toBytes(stopRow));
            if (StringUtils.isNotBlank(columnFamily) && columns != null && columns.length > 0) {
                byte[] colFamily = Bytes.toBytes(columnFamily);
                for (String column : columns) {
                    scan.addColumn(colFamily, Bytes.toBytes(column));
                }
            }
            ResultScanner rs = table.getScanner(scan);
            Map<String, Map<String, Map<String, String>>> rows = new LinkedHashMap<>();
            for (Result result : rs) {
                Map<String, Map<String, String>> map = resultToMap(result);
                rows.put(Bytes.toString(result.getRow()), map);
            }
            return rows;
        } catch (LabradorException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("扫描数据失败,table:%s", tableName);
            log.error(msg, e);
            throw new LabradorException(msg);
        }
    }

    /**
     * 扫描某个列族数据
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columnFamily
     * @param columns
     * @return void
     */
    public Map<String, Map<String, Map<String, String>>> scanColumnFamily(String tableName, String startRow, String stopRow, String columnFamily, String[] columns) {
        return scanColumnFamily(tableName, startRow, stopRow, columnFamily, columns, null);
    }

    private Map<String, Map<String, String>> resultToMap(Result result) throws UnsupportedEncodingException {
        List<Cell> cells = result.listCells();
        Map<String, Map<String, String>> kv = new HashMap<>();
        if (CollectionUtils.isEmpty(cells))
            return kv;
        for (Cell cell : cells) {
            String columnFamily = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell), "UTF-8");
            kv.putIfAbsent(columnFamily, new HashMap<>());
            kv.get(columnFamily).put(column, value);
        }
        return kv;
    }

    private String buildTableNameWithNameSpace(String tableName, String namespace) {
        if (StringUtils.isNotBlank(namespace))
            tableName = namespace + ":" + tableName;
        return tableName;
    }

    @PostConstruct
    private void init() throws IOException {
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
