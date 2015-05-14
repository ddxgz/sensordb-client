package com.inesa.sensordb.api;


import com.inesa.redis.connect.RedisConnectPool;
import com.inesa.redis.connect.SensordbSub;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by pc on 15-5-12.
 */
public class Main {
    public static String sensordb_ip = "122.144.166.103";
    public static int sensordb_port = 6677;

    public static String new_table_prefix = "new_table_";

    static int max=10;
    static String addr="10.200.46.245";
    static int port=7003;
    public static RedisConnectPool mypool;

    public static void main(String[] args){
//        BasicClient client = new BasicClient();
//        client.putdata();
//        client.getdata();
//        client.get_tables();
//        client.drop_create_table();
//        client.multi_put(30, 10000);
//        client.concurrent_run(1);

//
//        JsonConvertor jsonconv = new JsonConvertor();
//        Map<String, byte[]> values_map = new HashMap<String, byte[]>();
//        List<String> list_in = new LinkedList<String>();
//
//        SensordbSub myssb=new SensordbSub(addr, port);
//        while(myssb.listen()){
//            list_in = myssb.getRead();
//            if(!list_in.isEmpty()) {
//                String str_in = list_in.get(0);
//                System.out.println("redis str_in: " + str_in);
//                put_sensordb(str_in);
//            }
////            values_map = jsonconv.get_map(str_in);
////            System.out.println("jsonconv.convert values_map: " + values_map);
//        }

//        SensordbSub myssb=new SensordbSub(addr,port);
//        while(myssb.listen()){
//            System.err.print(myssb.getRead().toString()+"\n");
//        }

        test_interface();
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

        sensordb.get_json_record(/*new_table_prefix+"1"*/"table_test1", "sensor_in_json_file",
                "2015-05-12 01:00:00", "2015-05-12 04:00:00");

    }

}
