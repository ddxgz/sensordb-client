package com.inesa.sensordb.api;


import com.inesa.redis.connect.RedisConnectPool;
import com.inesa.redis.connect.SensordbSub;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        BasicClient client = new BasicClient();
//        client.putdata();
//        client.getdata();
        client.get_tables();
//        client.drop_create_table();
//        client.multi_put(30, 10000);
//        client.concurrent_run(1);

//        JsonConvertor jsonconv = new JsonConvertor();
//        try {
////            jsonconv.json2java(new FileReader("resource/jsonsample.json"));
//            jsonconv.sub_json(new FileReader("resource/jsonsample.json"), "values");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
////
//        Map<String, byte[]> values_map = new HashMap<String, byte[]>();
//        try {
//            values_map = jsonconv.convert(new FileReader("resource/jsonsample.json"));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        System.out.println("values_map: " + values_map);


//        SensordbSub myssb=new SensordbSub(addr, port);
//        while(myssb.listen()){
//            System.out.println("redis comming: " + myssb.getRead().toString());
//            values_map = jsonconv.convert(myssb.getRead().toString());
//            System.out.println("jsonconv.convert values_map: " + values_map);
//
//        }

//        test_interface();
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
//            for(int j=0; j<10; ++j)
//                sensordb.put_record(new_table_prefix + String.valueOf(i), jsonstr);
    }

}
