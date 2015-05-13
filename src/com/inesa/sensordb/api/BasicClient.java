package com.inesa.sensordb.api;

/**
 * Created by pc on 15-5-11.
 */

//import java.io.*;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.cesl.sensordb.client.Connection;
import org.cesl.sensordb.client.ResultSet;
import org.cesl.sensordb.client.Row;
import org.cesl.sensordb.core.Item;
import org.cesl.sensordb.exception.DBException;


public class BasicClient {
    public static String sensordb_ip = "122.144.166.103";
    public static int sensordb_post = 6677;
    public static List<String> table_list;
    public static List<String> table_list_t2;
    public static boolean table_t2_exist;
    public static String table_test_name = "new_table";
    public static int status;

    public static String sensorID = "sensor_test_";
    public static long sample_timestamp = 1431314021;
    public static double[] spacexyz = {1.2, 2.3, 3.4};
    public static Map<String, byte[]> sample_values = new HashMap();
    public static Map<String, byte[]> result_map = new HashMap();
//    public static String value1 = "value1";
//    public static byte[] value1_b = value1.getBytes();

//    public static Item item1 = new Item();
    public static List<Item> items;
    public static ResultSet result_set;
    public static Date date_now = new Date();
    public static double[] timecsv = new double[10];

    public void concurrent_run(int num) {
//        final int num = 10;
        final CountDownLatch begin = new CountDownLatch(1);
        final CountDownLatch end = new CountDownLatch(num);
        List<Double> runningtime = new ArrayList<Double> ();


        for (int i = 0; i < num; i++) {
            new Thread(new MyWorker(i, begin, end)).start();
        }

        // 睡眠0.1秒
        try {
            Thread.sleep(100);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        System.out.println("开始进行并发测试");
        begin.countDown();
        long startTime = System.currentTimeMillis();

        try {
            end.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            long endTime = System.currentTimeMillis();
            System.out.println("结束并发测试 !");
            System.out.println("花费时间: " + (endTime - startTime));
            FileWriter timeCSV = null;
            try {
                timeCSV = new FileWriter("timeCSV.csv");
                writeCSV(timecsv, timeCSV);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
    class MyWorker implements Runnable {
        final CountDownLatch begin;
        final CountDownLatch end;
        final int id;
        BasicClient client;//= new BasicClient();
        double[] time_multi_put;


        public MyWorker(final int id, final CountDownLatch begin,
                        final CountDownLatch end) {
            this.id = id;
            this.begin = begin;
            this.end = end;
//            Connection conn = new Connection(sensordb_ip, sensordb_post);
//            this.conn = conn;
//            this.conn.connect();
            BasicClient client = new BasicClient();
            this.client = client;
        }

        @Override
        public void run() {
            double pro_time = 0;
            try {

                System.out.println(this.id + " ready !");
                begin.await();
                // execute your logic
                System.out.println(this.id + " start !");
                double starttime = System.currentTimeMillis();
                time_multi_put = client.multi_put(2, 10000);

//                sample_values.put("key3", "value5".getBytes());
//                double starttime = System.currentTimeMillis();
//                for(int i=0;i<1000;++i)
//                    client.put_singledata(sample_values);
                pro_time = System.currentTimeMillis() - starttime;

//                Thread.sleep((long) (Math.random() * 1000));
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                System.out.println(this.id + " 完成测试 !");
                timecsv[id] = time_multi_put[0];
//                timecsv[id] = pro_time;
                end.countDown();
            }
        }
    }

    public void writeCSV(double[] measure1, FileWriter csv) throws IOException {
        try {
            //FileWriter csv = new FileWriter("/home/ddxgz1/a/nodes.csv");
            int length = measure1.length;

            csv.write("header"+"\n");
            for (int i = 0; i < length; i++) {
                csv.append(measure1[i] + "\n");
            }
            csv.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public double[] multi_put(int num, int item_num) {
        double[] pro_time = new double[num];
        Connection conn = new Connection(sensordb_ip, sensordb_post);

        try {
            conn.connect();
//            long starttime = System.currentTimeMillis();
//            System.out.println("start time: " + starttime);
            for (int i = 0; i < num; ++i) {
//                sample_values.put("key_test" + String.valueOf(i), ("value_" + String.valueOf(i)).getBytes());
//                System.out.println("i: " + i + "sample_values: " + sample_values);
                double starttime = System.currentTimeMillis();

                List<Item> items = getItems(item_num);
//                System.out.println("items: " + items);
                status = conn.batchPut(table_test_name, items);
                System.out.println("batchPut status: " + status);

//                System.out.println("date getTime: " + date_now.toString());
//                status = conn.put(table_test_name, sensorID.getBytes(),
//                        date_now.getTime(),
//                        spacexyz[0], spacexyz[1], spacexyz[2],
//                        sample_values);
                pro_time[i] = System.currentTimeMillis() - starttime;
//                System.out.println("pro_time time: " + pro_time[i]);

            }
//            pro_time[i] = System.currentTimeMillis() - starttime;
//            System.out.println("pro_time time: " + pro_time);
        } catch (Exception e) {
            e.printStackTrace();
            return new double[]{-1, -1};
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
            System.out.println("finally");
            conn.close();
        }
        return pro_time;
    }

    public List<Item> getItems(int num) {
//        Map<String, ByteBuffer> values = new HashMap();
        Date date = new Date();
        Item item = new Item();
        List<Item> items = new ArrayList<Item>();
        for (int i = 0; i < num; ++i) {
            Map<String, ByteBuffer> values = new HashMap();
            values.put("key_test1" + String.valueOf(i),
                    ByteBuffer.wrap(("value_" + String.valueOf(i)).getBytes()));
            item.sensorID = ByteBuffer.wrap(sensorID.concat("_d_")
                    .concat(String.valueOf(i)).getBytes());
            item.sampledts = date.getTime();
            item.x = spacexyz[0]+i;
            item.y = spacexyz[1]+i;
            item.z = spacexyz[2]+i;
            item.values = values;
            items.add(item);
//            System.out.println("item: " + item);
//            date.wait(5);
        }

        return items;
    }

    public void put_singledata(Map<String, byte[]> sample_values) {
        Connection conn = new Connection(sensordb_ip, sensordb_post);
        try {
            conn.connect();

//            sample_values.put("key3", "value5".getBytes());
//            sample_values.put("key4", "value6".getBytes());
            status = conn.put(table_test_name, sensorID.getBytes(),
                    date_now.getTime(),
                spacexyz[0], spacexyz[1], spacexyz[2], sample_values);
//            System.out.println("table: " + table_test_name + " -put status: "
//                    + status);

        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
//            System.out.println("finally");
            conn.close();
        }
    }

    public void putdata() {
        Connection conn = new Connection(sensordb_ip, sensordb_post);
        try {
            conn.connect();
            System.out.println("Connecting Done.");
            table_list = conn.listTableNames();
            System.out.println("table_list: " + table_list);

            result_set = conn.get(table_test_name,
                    "2015-05-11 01:00:00", "2015-06-12 23:00:00");
            System.out.println("table: " + table_test_name + " --get result: "
                    + result_set + " --error code: " + result_set.getErrCode()
                    + " --size: " + result_set.getSize());

//            for (int i = 0; i < result_set.getSize(); ++i) {
////                result_map = result_set.getRows(0,2);
//                System.out.println("table: " + table_test_name
//                        + " --get result next: " + result_set.toString());
//            }

//            table_list_t2 = conn.listTableNames("table");
//            System.out.println("listTableNames(\"test1$\"): "
//                    + table_list_t2);

//            table_t2_exist = conn.isExisted("table_test1");
//            System.out.println("isExisted(\"table_test1\"): " + table_t2_exist);
//
//            conn.createTable(table_test_name);
//            System.out.println("createTable: " + table_test_name);

//            conn.dropTables("/12$");
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
//            conn.createTable(table_test_name);
//            System.out.println("createTable: " + table_test_name);

//            sample_values.put("key3", "value5".getBytes());
//            sample_values.put("key4", "value6".getBytes());
//            status = conn.put(table_test_name, sensorID.getBytes(),
//                    date_now.getTime(),
//                spacexyz[0], spacexyz[1], spacexyz[2], sample_values);
//            System.out.println("table: " + table_test_name + " -put status: "
//                    + status);

//            List<Item> items = getItems(7);
//            System.out.println("items: " + items);
//            status = conn.batchPut(table_test_name, items);
//            System.out.println("table: " + table_test_name + " -put items status: "
//                    + status);
            System.out.println("date getTime: " + date_now.toString());

//            result_set = conn.get(table_test_name, sensorID.getBytes(),
//                    "2015-05-11 01:00:00", "2015-05-12 23:00:00");
//            System.out.println("table: " + table_test_name + " --get result: "
//                    + result_set + " --error code: " + result_set.getErrCode()
//                    + " --size: " + result_set.getSize());
//
//            for (int i = 0; i < result_set.getSize(); ++i) {
////                result_map = result_set.getRows(0,2);
//                System.out.println("table: " + table_test_name
//                        + " --get result next: " + result_set.toString());
//            }

//            System.out.println("return: " + multi_put(conn, 10));

        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
            System.out.println("finally");
            conn.close();
        }
    }

    public void getdata() {
        Connection conn = new Connection(sensordb_ip, sensordb_post);
        try {
            conn.connect();
            System.out.println("Connecting Done.");
            table_list = conn.listTableNames();
            System.out.println("table_list: " + table_list);

            result_set = conn.get(table_test_name,
                    "2015-05-12 06:29:43", "2015-05-12 12:50:00");
            System.out.println("table: " + table_test_name + " --get result: "
                    + result_set + " --error code: " + result_set.getErrCode()
                    + " --size: " + result_set.getSize());

//            List<Row> list_row = result_set.getRows(0,12);
//            for (int i = 0; i < 12; ++i) {
            while(result_set.next()){
//                Row row = result_set.getLong("ts");
//                Long gotlong = result_set.getLong("cf:id");

//                  List<Row> list_row = result_set.getRows(0,5);
//                double result_str = result_set.get("cf:id");

//                System.out.println("table: " + table_test_name
//                        + " --get result_str: " + result_str);
            }
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
            System.out.println("finally");
            conn.close();
        }
    }

    public void get_tables() {
        Connection conn = new Connection(sensordb_ip, sensordb_post);
        try {
            conn.connect();
            System.out.println("Connecting Done.");
            table_list = conn.listTableNames();
            System.out.println("table_list: " + table_list);

//            table_list_t2 = conn.listTableNames("^ta.*");
//            System.out.println("listTableNames(\"test1$\"): "
//                    + table_list_t2);

        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
            System.out.println("finally");
            conn.close();
        }
    }

    public void drop_create_table() {
        Connection conn = new Connection(sensordb_ip, sensordb_post);
        try {
            conn.connect();
            System.out.println("Connecting Done.");
            table_list = conn.listTableNames();
            System.out.println("table_list: " + table_list);

//            conn.createTable(table_test_name);
//            System.out.println("createTable: " + table_test_name);

//            conn.dropTables("/12$");
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
            conn.createTable("ta123");
            System.out.println("createTable: " + table_test_name);
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
            System.out.println("finally");
            conn.close();
        }
    }
}
