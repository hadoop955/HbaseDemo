/**
 * @Author: skm
 * @Date: 2019/4/27 17:04
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * java api操作远程的hbase服务
 */
public class Demo1 {
    private static Configuration configuration;
    private static HBaseAdmin hBaseAdmin;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://mycluster/hbase");
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 判断表是否存在
     */
    public static boolean isTableExist(String tableName) {
        try {
//            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            return hBaseAdmin.tableExists(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建表
     */
    public static void createTable(String tableName, String... colummFamily) {
        try {
//            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            //先判断表是否存在
            if (isTableExist(tableName)) {
                System.out.println("The table has already exist!");
            } else {
                //将表名变成字节类型，hbase只有一种数据类型就是byte
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                //向表中添加列族
                for (String cf : colummFamily) {
                    hTableDescriptor.addFamily(new HColumnDescriptor(cf));
                }
                //创建表
                hBaseAdmin.createTable(hTableDescriptor);
                System.out.println("表" + tableName + "已经创建成功了！");
                hBaseAdmin.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一张表
     */
    public static void dropTable(String tableName) {
        if (isTableExist(tableName)) {
            try {
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
                hBaseAdmin.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            System.out.println("表" + tableName + "压根不存在！");
        }
    }

    /**
     * 向表中插入数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    public static void addData(String tableName, String rowKey, String columnFamily, String column, String value) {
        try {
            //创建表对象
            HTable hTable = new HTable(configuration,tableName);
            //向表中插入数据
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
            hTable.put(put);
            hTable.close();
            System.out.println("插入成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 一次性删除多行数据
     * @param tableName
     * @param rows
     */
    public static void deleteMultiRow(String tableName,String... rows){
        try {
            HTable hTable = new HTable(configuration,tableName);
            List<Delete> deleteList = new ArrayList<Delete>();
            for (String row :rows){
                Delete delete = new Delete(Bytes.toBytes(row));
                deleteList.add(delete);
            }
            hTable.delete(deleteList);
            hTable.close();
            System.out.println("Successful deleted!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取一张表中的所有数据
     * @param tableName
     */
    public static void getAllRows(String tableName){
        try {
            HTable hTable = new HTable(configuration,tableName);
            //得到扫描region的对象
            Scan scan = new Scan();
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner){
                Cell[] cells = result.rawCells();
                for (Cell cell :cells){
                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            hTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
