package com.inesa.sensordb.api;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.gson.Gson;
import com.inesa.redis.connect.RedisConnectPool;
import com.inesa.redis.connect.SensordbSub;

import com.inesa.sensordb.api.test.AvailableTest;
import com.inesa.sensordb.api.test.ConcurrentTest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.cesl.sensordb.client.Connection;
import org.cesl.sensordb.client.ResultSet;
import org.cesl.sensordb.core.Item;
import org.cesl.sensordb.exception.DBException;

/**
 * Created by pc on 15-5-12.
 */
public class Main {
    public static String sensordb_ip = "122.144.166.103";
    public static int sensordb_port = 6677;
    public static String new_table_prefix = "trivial_table_";

    static int max = 10;
    static String addr = "10.200.46.245";
    static int port = 7000;
    public static RedisConnectPool mypool;


    public static Logger logger = Logger.getLogger(Main.class);


    public static void main(String[] args) throws DBException, InterruptedException {

//        Connection conn = new Connection(sensordb_ip, sensordb_port);
//        conn.connect();
//        conn.dropTables("^ava_test_table_.*");
//        conn.createTable(new_table_prefix+"1");
//        SensordbClient sensordb = new SensordbClient(sensordb_ip, sensordb_port);
//        SensordbClient sensordb = new SensordbClient(conn);
//
//        List<String> tables = new ArrayList<>();
//        tables = sensordb.tables();
//        System.out.println("tables: " + tables);
//
//        put_from_redis(conn);
//        put_from_redis_foreverconn(conn, new_table_prefix + "4");
//

//        ConcurrentTest test = new ConcurrentTest();
//        test.concurrent_run(4);
        AvailableTest test = new AvailableTest();
//        test.run_available_test();
//        test.fetch_performance_test(5);
        test.batchPut_performance_test(10);

//        multi_put(3,200000);

//        int[] nums = {10000, 10000/*, 5000, 10000, 50000, 100000, 200000, 300000*/};
//        for(int num:nums) {
////            logger.info("num: " + num);
//            for (int i = 0; i < 1; ++i) {
//                put_performance(conn, new_table_prefix + "1", 1000000);
//            }
//        }

//        test_interface();

//        get_performance(conn, new_table_prefix + "4",
//                "2015-05-29 17:11:00", "2015-05-29 17:55:00");


//        conn.connect();
////        logger.debug("drop");
////        conn.dropTable(new_table_prefix + "1");
////        logger.debug("create");
////
////        conn.createTable(new_table_prefix + "1");
////        List<String> tables = new ArrayList<>();
////        tables = sensordb.tables();
////        System.out.println("tables: " + tables);
////
//        logger.debug("get");
//
//        ResultSet result_set = conn.get(new_table_prefix + "1",
//                "2015-05-29 10:00:00", "2015-05-29 18:00:00");
//        System.out.println("result_set: " + result_set + " size:"
//                + result_set.getSize() + " errorcode:" +
//                result_set.getErrCode());
//        conn.close();

//        List < String > valuekeys = new ArrayList<>();
//        valuekeys.add("admin");
//        valuekeys.add("state");
//        valuekeys.add("type");
//        valuekeys.add("name");
//        List<String> result_json = sensordb.get_json_record(
//                new_table_prefix + "1",
////                "10.200.45.148",
//                "2015-05-10 01:00:00", "2015-06-29 18:00:00",
//                valuekeys);


//        test_conn_duration();

    }


    public static void multi_put(int num, int item_num) {
        Connection conn = new Connection(sensordb_ip, sensordb_port);

        try {
            conn.connect();
//            long starttime = System.currentTimeMillis();
//            System.out.println("start time: " + starttime);
            for (int i = 0; i < num; ++i) {
                long starttimewhole = System.currentTimeMillis();

                List<Item> items = getItems(item_num);
//                System.out.println("items: " + items);
                conn.batchPut(new_table_prefix + "1", items);
//                System.out.println("batchPut status: " + status);

                long endtimewhole = System.currentTimeMillis();
                logger.info("batchPut: " + item_num +
                        " items in 1 conn: "
                        + (endtimewhole - starttimewhole) + " ms");
            }
//            pro_time[i] = System.currentTimeMillis() - starttime;
//            System.out.println("pro_time time: " + pro_time);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            System.out.println("multi_put finally");
            conn.close();
        }
    }


    public static List<Item> getItems(int num) {
//        Map<String, ByteBuffer> values = new HashMap();
        Date date = new Date();
        Item item = new Item();
        List<Item> items = new ArrayList<Item>();
        for (int i = 0; i < num; ++i) {
            Map<String, ByteBuffer> values = new HashMap();
            values.put("key_test1" + String.valueOf(i),
                    ByteBuffer.wrap(("value_" + String.valueOf(i)).getBytes()));
            item.sensorID = ByteBuffer.wrap("testsendor".concat("_f_")
                    .concat(String.valueOf(i)).getBytes());
            item.sampledts = date.getTime();
            item.x = (item.sampledts%3+1)+i;
            item.y = (item.sampledts%3+2)+i;
            item.z = (item.sampledts%3+3)+i;
            item.values = values;
            items.add(item);

        }

        return items;
    }

    public static void test_conn_duration() throws DBException, InterruptedException {
        Connection conn = new Connection(sensordb_ip, sensordb_port);

        //        long starttimewhole = System.currentTimeMillis();
//        long duration = System.currentTimeMillis() - starttimewhole;;
//        conn.connect();
//        while(duration<1000000) {
//            Thread.sleep(10000);
//            ResultSet result_set2 = conn.get(new_table_prefix + "1",
//                    "2015-05-15 01:00:00", "2015-05-15 23:00:00");
//            System.out.println("result_set: " + result_set2 + " size:"
//                    + result_set2.getSize() + " errorcode:" +
//                    result_set2.getErrCode());
//            duration = System.currentTimeMillis() - starttimewhole;
//            logger.info("conn continues: "
//                    + duration/1000 + " s");
//        }
//        conn.close();

        long starttime = System.currentTimeMillis();
        long duration2 = System.currentTimeMillis() - starttime;
        ;
        conn.connect();
        ResultSet result_set2 = conn.get(new_table_prefix + "1",
                "2015-05-15 01:00:00", "2015-05-15 23:00:00");
        System.out.println("result_set2: " + result_set2 + " size:"
                + result_set2.getSize() + " errorcode:" +
                result_set2.getErrCode());
        while (duration2 < 1000000) {
            Thread.sleep(10000);
            duration2 = System.currentTimeMillis() - starttime;
            logger.info("conn without action continues: "
                    + duration2 / 1000 + " s");
        }
        ResultSet result_set3 = conn.get(new_table_prefix + "1",
                "2015-05-15 01:00:00", "2015-05-15 23:00:00");
        System.out.println("result_set3: " + result_set3 + " size:"
                + result_set3.getSize() + " errorcode:" +
                result_set3.getErrCode());
        conn.close();

    }


    public static void get_performance(Connection conn, String tablename,
                                       String query_starttime, String query_endtime)
            throws InterruptedException {
        JsonConvertor jsonconv = new JsonConvertor();
        Map<String, byte[]> values_map = new HashMap<>();
        List<String> list_in = new ArrayList<>();
//        Connection conn = new Connection(sensordb_ip, sensordb_port);
        SensordbClient sensordb = new SensordbClient(conn);
//        sensordb.start_conn_manager(sensordb.conn);

        String jsonstr = " ";
        List<String> json_list = new ArrayList<>();
        Gson gson = new Gson();
//        SensordbItem item = new SensordbItem();
        ResultItem item = new ResultItem();



//        ResultSet result_set = new ResultSet();
        try {
            conn.connect();
            for(int i=0;i<10;++i) {
                long starttimewhole = System.currentTimeMillis();

                ResultSet result_set = conn.get(tablename,
                        query_starttime, query_endtime);
                long endtimewhole = System.currentTimeMillis();
                logger.info("get " + result_set.getSize() + " items in 1 conn: "
                        + (endtimewhole - starttimewhole) + " ms");
                System.out.println("result_set: " + result_set + " size:"
                        + result_set.getSize() + " errorcode:" +
                        result_set.getErrCode());
//
//                starttimewhole = System.currentTimeMillis();
//                    while (result_set.next()) {
//                        result_set.getString("id");
//                        item.id = result_set.getString("id");
//                        item.ts = result_set.getLong("ts");
//                        item.x = result_set.getDouble("x");
//                        item.y = result_set.getDouble("y");
//                        item.z = result_set.getDouble("z");
////                String tst = result_set.getString("word_separators");
////                        for (int i=0; i<valuekeys.size(); ++i){
////                            item.values.put(valuekeys.get(i),
////                                    result_set.getString(valuekeys.get(i)));
////                        }
////                        System.out.println("item- id:" + item.id + " - ts:" + item.ts/*+
////                        " tst:"+tst+" x:"+item.x+" y:"+item.y+" z:"+item.z*/
////                                +"values: "+item.values);
////                        jsonstr = gson.toJson(item);
////                        json_list.add(jsonstr);
//                    }
//                endtimewhole = System.currentTimeMillis();
//                logger.info("scan " + result_set.getSize() + " items in 1 conn: "
//                        + (endtimewhole - starttimewhole) + " ms");
            }
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            System.out.println("get_record finally");
            conn.close();
        }




    }


    public static void put_performance(Connection conn, String tablename,
                                       int num)
            throws InterruptedException, DBException {
        JsonConvertor jsonconv = new JsonConvertor();
        Map<String, byte[]> values_map = new HashMap<>();
        List<String> list_in = new ArrayList<>();
//        Connection conn = new Connection(sensordb_ip, sensordb_port);
        SensordbClient sensordb = new SensordbClient(conn);
//        sensordb.start_conn_manager(sensordb.conn);

//        while (true) {
//            Random rand1 = new Random();
//            int random1 = rand1.nextInt(100);
//            System.out.println("put_performance sleep " + random1 * 100 + "ms");
//            Thread.sleep(random1 * 100);
            try {
                conn.connect();
//                List<SensordbItem> items = get_sensordb_items((random1 % 3 + 1) * num * 10);
                List<SensordbItem> items = get_sensordb_items(num);
                long starttimewhole = System.currentTimeMillis();

//            sensordb.long_put(tablename, list_in);
                sensordb.long_put_items(tablename, items);

//                try {
//                    sensordb.long_put_items(tablename, items);
//                } catch (DBException e) {
//                    e.printStackTrace();
//                }

//            for(SensordbItem item : items){
//                conn.put(new_table_prefix + "1", item.sensorID,
//                            item.timestamp,
//                            item.x, item.y, item.z, item.values);
//                }

                long endtimewhole = System.currentTimeMillis();
//                logger.info("put " + (random1 % 3 + 1) * num * 10 + " items in 1 conn: "
//                        + (endtimewhole - starttimewhole) + " ms");
                logger.info("put " + num + " items in 1 conn: "
                        + (endtimewhole - starttimewhole) + " ms");

            } finally {
//            ResultSet result_set = conn.get(new_table_prefix + "1",
//                    "2015-05-13 01:00:00", "2015-05-14 23:00:00");
//            System.out.println("result_set: " + result_set + " size:"
//                    + result_set.getSize() + " errorcode:" +
//                    result_set.getErrCode());
                System.out.println("put_performance finally");
            conn.close();
            }
//        }
    }


    public static List<SensordbItem> get_sensordb_items(int num) {
//        Map<String, ByteBuffer> values = new HashMap();
        double[] spacexyz = {1.2, 2.3, 3.4};
        String sensorID = "sn";
        Date date = new Date();
        SensordbItem item = new SensordbItem();
        List<SensordbItem> items = new ArrayList<SensordbItem>();
        for (int i = 0; i < num; ++i) {
            Map<String, byte[]> values = new HashMap<String, byte[]>();
            values.put("key_test1" + String.valueOf(i),
                    ("value_" + String.valueOf(i)).getBytes());
            item.sensorID = (sensorID.concat("_e_").concat(String.valueOf(i))
                    .getBytes());
            item.timestamp = date.getTime();
            item.x = spacexyz[0] + i;
            item.y = spacexyz[1] + i;
            item.z = spacexyz[2] + i;
            item.values = values;
            items.add(item);
        }

        return items;
    }


    public static void put_from_redis(Connection conn, String tablename)
            throws InterruptedException, DBException {
        List<String> list_in = new ArrayList<>();
//        Connection conn = new Connection(sensordb_ip, sensordb_port);
        SensordbClient sensordb = new SensordbClient(conn);
        int cnt = 0;
        long receive_cnt = 0;

        try {
//            conn.connect();
//            sensordb.connect();
            sensordb.start_conn_manager(sensordb.conn);
            SensordbSub myssb = new SensordbSub(addr, port);
            while (myssb.listen()) {
//                Thread.sleep(500);
                list_in = myssb.getRead();
                if (list_in.size() > 0) {
                    long starttimewhole = System.currentTimeMillis();

                    sensordb.long_put(tablename, list_in);


//                    for (String str_in : list_in){
//                        System.out.println("redis str_in: " + str_in);
//                        //     put_sensordb(str_in);
//                        SensordbItem item = new SensordbItem(str_in);
//                        conn.put(new_table_prefix + "2", item.sensorID,
//                                item.timestamp,
//                                item.x, item.y, item.z, item.values);
//                        ++receive_cnt;
//                    }

                    long endtimewhole = System.currentTimeMillis();
                    logger.info("from redis 1 list: " + list_in.size()
                            + " item in 1 conn: "
                            + (endtimewhole - starttimewhole) + " ms");
                    list_in.clear();
//                ++cnt;
//                if(cnt>=10)
//                    break;
                }
                logger.info("from redis all:" + receive_cnt);
            }
//        } catch (DBException e) {
//            e.printStackTrace();
//            System.exit(-1);
        } finally {
            System.out.println("put_from_redis finally");
//            conn.close();
//            sensordb.close();
        }
    }


    public static void put_from_redis_foreverconn(Connection conn,
                                                  String tablename)
            throws InterruptedException, DBException {

        List<String> list_in = new ArrayList<>();
        SensordbClient sensordb = new SensordbClient(conn);
        long receive_cnt = 0;

        try {
            conn.connect();
            sensordb.start_conn_manager(sensordb.conn);
            SensordbSub myssb = new SensordbSub(addr, port);
            while (myssb.listen()) {
//                Thread.sleep(500);
                list_in = myssb.getRead();
                if (list_in.size() > 0) {
                    long starttimewhole = System.currentTimeMillis();

                    for (String str_in : list_in) {
                        System.out.println("redis str_in: " + str_in);
                        //     put_sensordb(str_in);
                        SensordbItem item = new SensordbItem(str_in);
                        conn.put(tablename, item.sensorID,
                                item.timestamp,
                                item.x, item.y, item.z, item.values);
                        ++receive_cnt;
                    }

                    long endtimewhole = System.currentTimeMillis();
                    logger.info("from redis 1 list: " + list_in.size()
                            + " item in 1 conn: "
                            + (endtimewhole - starttimewhole) + " ms");
                    list_in.clear();
//                ++cnt;
//                if(cnt>=10)
//                    break;
                }
                logger.info("from redis all:" + receive_cnt);
            }
        } finally {
            System.out.println("put_from_redis_foreverconn finally");
            conn.close();
        }
    }


    public static void put_sensordb(String str_in) {
        SensordbClient sensordb = new SensordbClient(sensordb_ip, sensordb_port);
        List<String> tables = new ArrayList<String>();
        tables = sensordb.tables();
        System.out.println("tables: " + tables);

        sensordb.put_record(new_table_prefix + "1", str_in);
    }

    public static void test_interface() {
        SensordbClient sensordb = new SensordbClient(sensordb_ip, sensordb_port);
        List<String> tables = new ArrayList<String>();
        tables = sensordb.tables();
        System.out.println("tables: " + tables);

//        for (int i=0; i<10; ++i)
//            sensordb.create_table(new_table_prefix + String.valueOf(i));
//        tables = sensordb.tables();
//        System.out.println("tables: " + tables);

//        for (int i=0; i<10; ++i)
//            sensordb.delete_table(new_table_prefix + String.valueOf(i));
//        tables = sensordb.tables();
//        System.out.println("tables: " + tables);

        String jsonstr = new String();
        try {
            JsonConvertor jsonconv = new JsonConvertor();
            jsonstr = jsonconv.jsonstr(
                    new FileReader("resource/jsonsample.json"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
//        for(int i=0; i<10; ++i)
//            sensordb.put_record(new_table_prefix + String.valueOf(i), jsonstr);
        sensordb.put_record(new_table_prefix + String.valueOf(1), jsonstr);

//        List<String> valuekeys = new ArrayList<>();
//        valuekeys.add("word_separators");
//        valuekeys.add("font_size");
//        valuekeys.add("fold_buttons");
//        List<String> records_json = sensordb.get_json_record(new_table_prefix+"1"
//        /*"table_test1"*/, "sensor_in_json_file",
//                "2015-05-12 01:00:00", "2015-05-14 04:00:00",
//                valuekeys);
//        System.out.println("records_json: " + records_json);

//        PropertyConfigurator.configure("log4j.properties");
//        logger.info("Hello, World!");

    }

    public void get_env(){
        final Properties p = System.getProperties();
        final Enumeration e = p.keys();
        while (e.hasMoreElements())
        {
            final String prt = (String) e.nextElement();
            final String prtvalue = System.getProperty(prt);
            System.out.println(prt + ":" + prtvalue);
        }
    }
}
