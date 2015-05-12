package com.inesa.sensordb.api;


import com.inesa.redis.connect.RedisConnectPool;
import com.inesa.redis.connect.SensordbSub;

/**
 * Created by pc on 15-5-12.
 */
public class Main {
    static int max=10;
    static String addr="10.200.46.245";
    static int port=7003;
    public static RedisConnectPool mypool;

    public static void main(String[] args){
        BasicClient client = new BasicClient();
//        client.putdata();
//        client.getdata();
        client.getTables();
//        client.drop_create_table();
//        client.multi_put(30, 10000);
//        client.concurrent_run(1);

//        JsonConvertor jsonconv = new JsonConvertor();
//        jsonconv.convert();


        SensordbSub myssb=new SensordbSub(addr,port);
        while(myssb.listen()){
            System.out.println("redis comming: " + myssb.getRead().toString());
        }
    }


}
