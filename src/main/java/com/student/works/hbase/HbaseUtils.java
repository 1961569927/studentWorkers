package com.student.works.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseUtils {
    public static Configuration conf;
    public static Connection conn;

    //初始化配置，每次开启链接只调用一次，创建链接
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.zookeeper.quorum","jikehadoop01,jikehadoop02,jikehadoop03");

        try {
            conn = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表，以及多列族，这里不涉及列，
     * @param tableName  表名，表空间需要自己创建
     * @param ColumnFamily  列族，多个列族用数组的形式传入
     * @throws IOException
     */
    public  static void createTable(String tableName, String... ColumnFamily) throws  IOException{
        Admin admin = conn.getAdmin();

        // hbase 表构造器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        List<ColumnFamilyDescriptor> list = new ArrayList<ColumnFamilyDescriptor>();

        for (String family : ColumnFamily) {
            //hbase 列族构造器，将列族一个一个添加到表构造器中
            ColumnFamilyDescriptor cf1Builder = ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(family));
            list.add(cf1Builder);
        }
        tableDescriptorBuilder.setColumnFamilies(list);
        TableDescriptor des = tableDescriptorBuilder.build();

        //如果表存在，则跳过，不存在，直接创建
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table has already exists!");
        }else{
            admin.createTable(des);
            System.out.println("Table Create Success!");
            admin.close();
        }
    }

    /**
     * 插入数据  相同rowKey的会覆盖，保留时间戳，作为版本号
     * @param tablename  表名
     * @param rowkey hbase 表的rowKey
     * @param ColumnFamilys  列族名称
     * @param columns 列名
     * @param values 值
     * @throws IOException
     */
    public static void insertData(String tablename, String rowkey, String ColumnFamilys, String[] columns,
                                  String[] values) throws IOException{
        Table table = conn.getTable(TableName.valueOf(tablename));

        //插入的数据封装成Put对象，传入到hbase表中
        Put put = new Put(Bytes.toBytes(rowkey));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(ColumnFamilys),Bytes.toBytes(columns[i]),Bytes.toBytes(values[i]));
        }

        table.put(put);
        System.out.println("data inserted!");
        table.close();
    }

    /**
     * 删除一条数据 根据rowKey 删除数据
     * @param tablename  表名
     * @param rowKey rowKey
     * @throws IOException
     */
    public static void deleteData(String tablename,String rowKey) throws  IOException{
        Table table = conn.getTable(TableName.valueOf(tablename));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
        System.out.println("delete success!");
    }

    /**
     * 根据表名去表扫描
     * @param tablename 表名
     * @throws IOException
     */
    public static void scanTable(String tablename) throws IOException {
        // Scan类内部含有多个查询方法，这里就用最简单的，全表扫描的方式，直接返回全部数据
        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf(tablename));
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            // cell 是 hbase 存储的最小单元，这里是直接读取的cell数据
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        "行键:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                                "列族:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                                "列名:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                                "值:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                                "时间戳:" + cell.getTimestamp());
            }
            results.close();
        }
    }

    /**
     * 指定rowKey 查询
     * @param tablename 表名
     * @param rowkey rowKey
     * @throws IOException
     */
    public static void scanRow(String tablename, String rowkey) throws  IOException{
        Get get= new Get(Bytes.toBytes(rowkey));
        Table table = conn.getTable(TableName.valueOf(tablename));
        Result result = table.get(get);
        if (result == null) {
            System.out.println(" row = " + rowkey + " , 不存在 ");
        }else {
            // 和上面类似，这里返回的是一个cell的集合
            for (Cell cell : result.listCells()) {
                System.out.println(
                        "行键:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                                "列族:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                                "列名:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                                "值:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                                "时间戳:" + cell.getTimestamp());
            }
        }
    }

}
