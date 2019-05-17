package weibo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: skm
 * @Date: 2019/5/7 16:21
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class WeiBo {
    //定三张表  分别是 1.微博内容表 2.用户关系表 3.微博收件箱表
    private static final byte[] TABLE_CONTENT = Bytes.toBytes("weibo:content");
    private static final byte[] TABLE_RELATIONS = Bytes.toBytes("weibo:relations");
    private static final byte[] TABLE_RECEIVE_EMAIL = Bytes.toBytes("weibo:receive_email");

    //创建操作hbase的配置信息对象
    private static Configuration configuration;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://mycluster/hbase");
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");

    }

    /**
     * 初始化命名空间namespace
     */
    public void initNamespace() {
        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            //给命名空间增加额外配置信息
            NamespaceDescriptor weibo = NamespaceDescriptor.create("weibo")
                    .addConfiguration("author", "xiaoming")
                    .addConfiguration("create_time", String.valueOf(System.currentTimeMillis()))
                    .build();
            hBaseAdmin.createNamespace(weibo);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hBaseAdmin != null) {
                try {
                    hBaseAdmin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 创建微博内容表weibo:content
     */
    public void createTableContent() {
        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            //创建表描述
            HTableDescriptor content = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));
            //创建列族描述
            HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("info"));
            //设置块缓存
            info.setBlockCacheEnabled(true);
            //设置块缓存大小为2M
            info.setBlocksize(2097152);
            //设置压缩方式
            //info.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
            //设置版本确界
            info.setMinVersions(1);
            info.setMaxVersions(1);
            content.addFamily(info);
            hBaseAdmin.createTable(content);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hBaseAdmin != null) {
                try {
                    hBaseAdmin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建用户关系表weibo:relations
     */
    public void createTableRelations() {
        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            //创建表描述
            HTableDescriptor relations = new HTableDescriptor(TableName.valueOf(TABLE_RELATIONS));
            //创建列族描述,关注人的列族
            HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("follow"));
            //设置块缓存
            info.setBlockCacheEnabled(true);
            //设置块缓存大小为2M
            info.setBlocksize(2097152);
            //设置压缩方式
            //info.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
            //设置版本确界
            info.setMinVersions(1);
            info.setMaxVersions(1);

            //创建粉丝列族fans
            HColumnDescriptor fans = new HColumnDescriptor(Bytes.toBytes("fans"));
            //设置块缓存
            fans.setBlockCacheEnabled(true);
            //设置块缓存大小为2M
            fans.setBlocksize(2097152);

            fans.setMinVersions(1);
            fans.setMaxVersions(1);

            relations.addFamily(info);
            relations.addFamily(fans);
            hBaseAdmin.createTable(relations);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hBaseAdmin != null) {
                try {
                    hBaseAdmin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建用户收件箱表weibo:receive_email
     */
    public void createTableEmail() {
        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            //创建表描述
            HTableDescriptor email = new HTableDescriptor(TableName.valueOf(TABLE_RECEIVE_EMAIL));
            //创建列族描述
            HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("info"));
            //设置块缓存
            info.setBlockCacheEnabled(true);
            //设置块缓存大小为2M
            info.setBlocksize(2097152);
            //设置压缩方式
            //info.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
            //设置版本确界
            info.setMinVersions(1);
            info.setMaxVersions(1);
            email.addFamily(info);
            hBaseAdmin.createTable(email);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hBaseAdmin != null) {
                try {
                    hBaseAdmin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 发布一条微博
     * 1.首先微博内容表，content表中数据+1
     * 2.用户收件箱表中加入微博的rowkey
     */
    public void publishContent(String userId , String content){
        //创建client连接表的连接
        HConnection connection = null;
        try {
            connection = HConnectionManager.createConnection(configuration);
            //1..微博content表里面添加数据，首先获得微博内容表的描述
            HTableInterface conyentTBL = connection.getTable(TableName.valueOf(TABLE_CONTENT));
            //自定义rowkey
            long timeStamp = System.currentTimeMillis();
            String rowKey = userId + "_" + timeStamp;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes("info"),Bytes.toBytes("content"),timeStamp,Bytes.toBytes(content));
            conyentTBL.put(put);
            //2.向微博收件箱表email中加入发布消息的rowKey
            //得到用户关系表，得到当前用户有哪些粉丝
            HTableInterface relationsTBL = connection.getTable(TableName.valueOf(TABLE_RELATIONS));
            //取出目标数据
            Get get= new Get(Bytes.toBytes(userId));
            get.addFamily(Bytes.toBytes("fancs"));

            Result result = relationsTBL.get(get);
            List<byte[]> fans = new ArrayList<byte[]>();
            //遍历取出当前发布微博用户的所有粉丝的数据
            for (Cell cell : result.rawCells()){
                fans.add(CellUtil.cloneQualifier(cell));
            }
            //如果该用户没有数据，则返回
            if (fans.size() == 0){
                return;
            }else {
                //开始操作收件箱表
                HTableInterface recTBL = connection.getTable(TableName.valueOf(TABLE_RECEIVE_EMAIL));
                List<Put> puts = new ArrayList<Put>();
                for (byte[] fan : fans){
                    Put fanPut = new Put(fan);
                    fanPut.add(Bytes.toBytes("info"),Bytes.toBytes(userId),timeStamp,Bytes.toBytes(rowKey));
                    puts.add(fanPut);
                }
                recTBL.put(puts);
            }



        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (connection != null){
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * 关注用户
     * 1.在微博关系用户表中，对当前的主动操作的用户添加新的关注的好友
     * 2.在微博关系用户表中，对被关注的用户添加当前操作的用户
     * 3.当前操作用户的微博收件箱中添加所关注的用户发布的微博rowKey
     */
    public void addAttends(String userId,String... attends){
        //参数过滤
        if (attends == null ||
        attends.length <= 0 ||
        userId == null ||
        userId.length()<=0){
            return;
        }else {
            HConnection connection = null;
            try {
                connection = HConnectionManager.createConnection(configuration);
                //连接到用户关系表
                HTableInterface relationsTBL = connection.getTable(TableName.valueOf(TABLE_RELATIONS));
                List<Put> puts = new ArrayList<Put>();
                //在微博关系用户表中，添加新关注的好友
                Put attendPut = new Put(Bytes.toBytes(userId));
                for (String attend :attends){
                    //为当前用户添加关注的人
                    attendPut.add(Bytes.toBytes("attends"),Bytes.toBytes(attend),Bytes.toBytes(attend));
                    //为被关注的人，添加粉丝
                    Put fansPut = new Put(Bytes.toBytes(attend));
                    fansPut.add(Bytes.toBytes("fans"),Bytes.toBytes(userId),Bytes.toBytes(userId));
                    //将所有关注的人一个个的添加到puts（List）
                    puts.add(attendPut);
                    puts.add(fansPut);
                }
                relationsTBL.put(puts);
                //3.微博收件箱添加关注的用户发布微博内容content的rowKey
                HTableInterface conentTBL = connection.getTable(TableName.valueOf(TABLE_RELATIONS));
                Scan scan = new Scan();
                //用来存放取出来的关注的人所发的微博的rowkey
                List<byte[]> rowkeys = new ArrayList<byte[]>();

                for (String attend : attends){
                    //过滤扫描rowkey，即，前置位匹配被关注的人的userid
                    RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(attend + "_"));
                    //为扫描对象指定过滤规则
                    scan.setFilter(rowFilter);
                    //通过扫描对象得到scanner
                    ResultScanner resultScanner = conentTBL.getScanner(scan);
                    //迭代器遍历扫描出来的结果集
                    Iterator<Result> iterator = resultScanner.iterator();
                    while(iterator.hasNext()){
                        //取出每一个符合扫描结果的哪一行数据
                        Result result = iterator.next();
                        for (Cell cell : result.rawCells()){
                            //将得到的rowkey放置于集合容器中
                            rowkeys.add(CellUtil.cloneRow(cell));
                        }
                    }
                }
                //4.将取出的微博rowkey放置于当前操作用户的收件箱中
                if (rowkeys.size()<=0){
                    return;
                }else{
                    HTableInterface recTBL = connection.getTable(TableName.valueOf(TABLE_RECEIVE_EMAIL));
                    //用于存放多个关注的用户的发布的多条微博rowkey信息
                    List<Put> recPuts = new ArrayList<Put>();
                    for (byte[] rk : rowkeys){
                        Put put = new Put(Bytes.toBytes(userId));
                        //userId_timeStamp
                        String rowKey = Bytes.toString(rk);
                        //借助userId
                        String attendUID = rowKey.substring(0,rowKey.indexOf("_"));
                        long timeStamp = Long.parseLong(rowKey.substring(rowKey.indexOf("_")+ 1));
                        //将微博rowkey添加到指定的单元格中
                        put.add(Bytes.toBytes("info"),Bytes.toBytes(attendUID),timeStamp,rk);
                        recPuts.add(put);
                    }
                    recTBL.put(recPuts);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (connection != null){
                    try {
                        connection.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    /**
     * 创建表的方法
     */
    public static void ininTable() {
        WeiBo weiBo = new WeiBo();
        weiBo.initNamespace();
        weiBo.createTableContent();
        weiBo.createTableRelations();
        weiBo.createTableEmail();

    }

    public static void main(String[] args) {
        ininTable();
    }
}


