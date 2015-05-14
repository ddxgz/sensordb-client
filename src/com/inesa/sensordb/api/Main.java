package com.inesa.sensordb.api;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.*;

import com.inesa.redis.connect.RedisConnectPool;
import com.inesa.redis.connect.SensordbSub;

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
    public static String new_table_prefix = "new_table_";

    static int max=10;
    static String addr="10.200.46.245";
    static int port=6379;
    public static RedisConnectPool mypool;


    public static Logger logger = Logger.getLogger(Main.class);


    public static void main(String[] args) throws DBException {
//        BasicClient client = new BasicClient();
//        client.putdata();
//        client.getdata();
//        client.get_tables();
//        client.drop_create_table();
//        client.multi_put(30, 10000);
//        client.concurrent_run(1);

        Connection conn = new Connection(sensordb_ip, sensordb_port);
//        conn.connect();
//        SensordbClient sensordb = new SensordbClient(sensordb_ip, sensordb_port);
        SensordbClient sensordb = new SensordbClient(conn);

        List<String> tables = new ArrayList<>();
        tables = sensordb.tables();
        System.out.println("tables: " + tables);
//
//        put_from_redis(conn);

        for(int i=0; i<10; ++i)
            put_performance(conn, 1000);


        conn.connect();
        ResultSet result_set = conn.get(new_table_prefix + "1",
                            "2015-05-14 05:55:00", "2015-05-14 23:00:00");
        System.out.println("result_set: " + result_set + " size:"
                + result_set.getSize() + " errorcode:" +
                result_set.getErrCode());
        conn.close();

//        SensordbSub myssb=new SensordbSub(addr,port);
//        while(myssb.listen()){
//            System.err.print(myssb.getRead().toString()+"\n");
//        }

//        test_interface();
    }


    public static void put_performance(Connection conn, int num) {
        JsonConvertor jsonconv = new JsonConvertor();
        Map<String, byte[]> values_map = new HashMap<>();
        List<String> list_in = new ArrayList<>();
//        Connection conn = new Connection(sensordb_ip, sensordb_port);

        try {
            conn.connect();
            List<SensordbItem> items = SensordbItems(num);
            long starttimewhole = System.currentTimeMillis();

            for(SensordbItem item : items){
                conn.put(new_table_prefix + "1", item.sensorID,
                            item.timestamp,
                            item.x, item.y, item.z, item.values);
                }
            long endtimewhole = System.currentTimeMillis();
            logger.info(num + " item in 1 conn: "
                    + (endtimewhole - starttimewhole) + " ms");
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            ResultSet result_set = conn.get(new_table_prefix + "1",
//                    "2015-05-13 01:00:00", "2015-05-14 23:00:00");
//            System.out.println("result_set: " + result_set + " size:"
//                    + result_set.getSize() + " errorcode:" +
//                    result_set.getErrCode());
            System.out.println("put_performance finally");
            conn.close();
        }
    }

    public static List<SensordbItem> SensordbItems(int num) {
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
            item.sensorID = (sensorID.concat("_d_").concat(String.valueOf(i))
                    .getBytes());
            item.timestamp = date.getTime();
            item.x = spacexyz[0]+i;
            item.y = spacexyz[1]+i;
            item.z = spacexyz[2]+i;
            item.values = values;
            items.add(item);
        }

        return items;
    }

    public static void put_from_redis(Connection conn) {
        List<String> list_in = new ArrayList<>();
//        Connection conn = new Connection(sensordb_ip, sensordb_port);
        int cnt = 0;
        long receive_cnt = 0;

        try {
//            conn.connect();
            SensordbSub myssb=new SensordbSub(addr, port);
            while(myssb.listen()){
                list_in = myssb.getRead();
                if(list_in.size()>0) {
                    long starttimewhole = System.currentTimeMillis();
                    for (String str_in : list_in){
                        System.out.println("redis str_in: " + str_in);
                        //     put_sensordb(str_in);
                        SensordbItem item = new SensordbItem(str_in);
                        conn.put(new_table_prefix + "2", item.sensorID,
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
//        } catch (DBException e) {
//            e.printStackTrace();
//            System.exit(-1);
        } finally {
            System.out.println("put_from_redis finally");
//            conn.close();
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

//        String jsonstr = new String();
//        try {
//            JsonConvertor jsonconv = new JsonConvertor();
//            jsonstr = jsonconv.jsonstr(
//                    new FileReader("resource/jsonsample.json"));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        for(int i=0; i<10; ++i)
//            sensordb.put_record(new_table_prefix + String.valueOf(i), jsonstr);

        List<String> valuekeys = new ArrayList<>();
        valuekeys.add("word_separators");
        valuekeys.add("font_size");
        valuekeys.add("fold_buttons");
        List<String> records_json = sensordb.get_json_record(new_table_prefix+"1"
        /*"table_test1"*/, "sensor_in_json_file",
                "2015-05-12 01:00:00", "2015-05-14 04:00:00",
                valuekeys);
        System.out.println("records_json: " + records_json);

//        PropertyConfigurator.configure("log4j.properties");
//        logger.info("Hello, World!");

    }

}
