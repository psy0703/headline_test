package com.dgmall.sparktest.dgmallTestV2.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;


public class HbaseClient {
    private  Admin admin;
    private  Connection conn ;

    public HbaseClient() {
        // 创建hbase配置对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://dg-bigdata/hbase");
        //使用eclipse时必须添加这个，否则无法定位
        conf.set("hbase.zookeeper.quorum","dev-node01,dev-node02,dev-node03");
//        conf.set("hbase.client.scanner.timeout.period", config.getString("hbase.client.scanner.timeout.period"));
//        conf.set("hbase.rpc.timeout", config.getString("hbase.rpc.timeout"));
        try {
            conn = ConnectionFactory.createConnection(conf);
            // 得到管理程序
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    private static HbaseClient ourInstance = new HbaseClient();
//
//    public static HbaseClient getInstance() {
//        return ourInstance;
//    }

    /**
     * 插入数据，create "userflaginfo,"baseinfo"
     * create "tfidfdata,"baseinfo"
     */
    public  void put(String tablename, String rowkey, String famliyname, Map<String,String> datamap) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Put put = new Put(rowkeybyte);
        if(datamap != null){
            Set<Map.Entry<String,String>> set = datamap.entrySet();
            for(Map.Entry<String,String> entry : set){
                String key = entry.getKey();
                Object value = entry.getValue();
                put.addColumn(Bytes.toBytes(famliyname), Bytes.toBytes(key), Bytes.toBytes(value+""));
            }
        }
        table.put(put);
        table.close();
        System.out.println("ok");
    }

    /**
     *
     */
    public  String getdata(String tablename, String rowkey, String famliyname,String colum) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Get get = new Get(rowkeybyte);
        Result result =table.get(get);
        byte[] resultbytes = result.getValue(famliyname.getBytes(),colum.getBytes());
        if(resultbytes == null){
            return null;
        }

        return new String(resultbytes);
    }


    //  create 'match_item_features','fea'
    //  put 'match_item_features','123','fea:f','1,2,3,4,5'
    //  put 'match_item_features','456','fea:f','4,5,6,7,8'
    //  put 'match_item_features','789','fea:f','4,5,6,7,8'


    //  put ’<table name>’,’row1’,’<colfamily:colname>’,’<value>’



    public  Set<String> getColumnList(String tablename, String rowkey, String famliyname) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Get get = new Get(rowkeybyte);
        Result result =table.get(get);
        Cell[] cells = result.rawCells();
        Set<String> set=new HashSet<>();

        for (Cell cell : cells) {
//            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
//            String rowName = Bytes.toString(CellUtil.cloneRow(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
//            System.out.println("column = " + column);
//            System.out.println("rowName = " + rowName);
//            System.out.println("value = " + value);
//            System.out.println("family = " + family);
            set.add(column + "," + value);
        }
        return set;
    }



    public  List<String> getBatch(List<String> rowkeyList,String tableName,String famliyname,String colum) throws IOException {
        List<String> list = new ArrayList<>();
        List<Get> getList = new ArrayList();
        Table table = conn.getTable(TableName.valueOf(tableName));// 获取表
        for (String rowkey : rowkeyList) {//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>

//
//        for (Result result : results) {//对返回的结果集进行操作
//            for (Cell kv : result.rawCells()) {
//                String value = Bytes.toString(CellUtil.cloneValue(kv));
//                list.add(value);
//            }
//        }
//        return list;


        for (Result result : results) {//对返回的结果集进行操作
            byte[] resultbytes = result.getValue(famliyname.getBytes(),colum.getBytes());
            list.add(new String(resultbytes));
        }
        return list;
    }


//    Get get = new Get(rowkeybyte);
//    Result result =table.get(get);
//    byte[] resultbytes = result.getValue(famliyname.getBytes(),colum.getBytes());
//            if(resultbytes == null){
//        return null;
//    }
//
//            return new String(resultbytes);

    /**
     *
     */
    public void putdata(String tablename, String rowkey, String famliyname,String colum,String data) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        Put put = new Put(rowkey.getBytes());
        put.addColumn(famliyname.getBytes(),colum.getBytes(),data.getBytes());
        table.put(put);
    }


}



