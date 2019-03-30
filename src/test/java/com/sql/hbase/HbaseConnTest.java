package com.sql.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;


public class HbaseConnTest {

    @Test
    public void getConnTest(){
        HbaseUtils hbase = HbaseUtils.getInstance();
        System.out.println(hbase.GetConn().isClosed());
        HbaseUtils.close();
        System.out.println(hbase.GetConn().isClosed());
    }

    @Test
    public void getTable(){
        try{
            System.out.println(new Date());
            HbaseUtils hbase = HbaseUtils.getInstance();
            System.out.println(new Date());
            Table table = hbase.getTable("FileTable2");
            if(table == null){
                System.out.println("不存在");
                return;
            }
            System.out.println(new Date());
            System.out.println(table.getName().toString());
            table.close();
        } catch (Exception e ){
            e.printStackTrace();
        }
    }

    @Test
    public void getAllTable(){
        try{
            HbaseUtils hbase = HbaseUtils.getInstance();
            hbase.getAllTables();
        } catch (Exception e ){
            e.printStackTrace();
        }
    }

    @Test
    public void createTable() throws Exception {
        HbaseUtils hbase = HbaseUtils.getInstance();
//        hbase.createTable("FileTable2",new String[]{"fileInfo","saveInfo"});
//        hbase.createTable("xvjun_course_clickcount",new String[]{"CountInfo"});
        hbase.createTable("xvjun_search_course_clickcount",new String[]{"CountInfo"});

    }
//
//    @Test
//    public void addFileDetails() throws Exception {
//        HbaseUtils hbase = HbaseUtils.getInstance();
////        hbase.putData("FileTable2","rowkey1","fileInfo","name","file1.txt");
////        hbase.putData("FileTable2","rowkey1","fileInfo","size","123");
////        hbase.putData("FileTable2","rowkey1","fileInfo","type","txt");
////        hbase.putData("FileTable2","rowkey1","saveInfo","creater","xj");
////        hbase.putData("FileTable2","rowkey2","fileInfo","name","file2.json");
////        hbase.putData("FileTable2","rowkey2","fileInfo","size","345");
////        hbase.putData("FileTable2","rowkey2","fileInfo","type","json");
////        hbase.putData("FileTable2","rowkey2","saveInfo","creater","ljp");
//        hbase.putData("FileTable2","rowkey3","saveInfo","creater","ljp");
//    }
//
    @Test
    public void getFileDelatils() throws Exception {
        System.out.println(new Date());
        HbaseUtils hbase = HbaseUtils.getInstance();
        System.out.println(new Date());
        Result rs = hbase.getResult("xvjun_course_clickcount","20190225_code_123");
        System.out.println(new Date());
        if(rs != null){
            System.out.println("rowkey=" + Bytes.toString(rs.getRow()));
            System.out.println("filename=" + Bytes.toString(rs.getValue("CountInfo".getBytes(),"click_count".getBytes())));
        }
        System.out.println(new Date());
    }
//
//    @Test
//    public void rowFilterTest() throws IOException {
//        System.out.println(new Date());
//        HbaseUtils hbase = HbaseUtils.getInstance();
//        System.out.println(new Date());
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("rowkey1")));
//        //1
////        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,Arrays.asList(filter));
////        ResultScanner sc = hbase.getScanner("FileTable2","rowkey1","rowkey2",filterList);
//        //2
//        Table table = hbase.GetConn().getTable(TableName.valueOf("FileTable2"));
//        Scan scan = new Scan();
//        scan.setFilter(filter);
//        ResultScanner sc = table.getScanner(scan);
//
//
//        System.out.println(new Date());
//        if(sc != null){
//            for(Result rs : sc){
//                System.out.println(rs.toString());
//                System.out.println(Bytes.toString(rs.getRow()));
//                System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("name"))));
//                List<Cell> cells = rs.listCells();
//                for(Cell cell : cells){
//                    System.out.println(cell);
//                }
//            }
//            sc.close();
//        }
//    }
//
//    @Test
//    public  void profixFilterTest(){
//
//    }
}
