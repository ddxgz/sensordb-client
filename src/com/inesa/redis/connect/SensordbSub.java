package com.inesa.redis.connect;

import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;

/**
 * Created by shihj on 5/11/15.
 */
public class SensordbSub {
    public static RedisConnectPool mypool;
    public ArrayList<String> read=new ArrayList<String>();

    static int max=10;
    static String addr="10.200.46.245";
    static int port=7003;
    static String connectChannel="sensorDB";

    private RedisClusterSub sub;
    private static SensordbSubThread sdbst;
    private static volatile int message_comming=0;

    public SensordbSub(String redisAddr, int redisPort){
        max=1;
        addr=redisAddr;
        port=redisPort;
        mypool=new RedisConnectPool(1,addr,port);
        if (mypool.createPool()==0)
            System.err.print("CreatePool failed");
    }

    public SensordbSub(int maxlink,String redisAddr, int redisPort){
        max=maxlink;
        addr=redisAddr;
        port=redisPort;
        mypool=new RedisConnectPool(max,addr,port);
        if (mypool.createPool()==0)
            System.err.print("CreatePool failed");
        else
            sub= mypool.getRedisCLusterSub();
    }

    public boolean listen(){
        if(sub==null) {
            mypool.dumpPool(max,addr,port);
            sub = mypool.getRedisCLusterSub();
            if (sub==null)
                return false;
        }
        if(sdbst==null){
            sdbst=new SensordbSubThread();
            sdbst.start();
        }

        while(message_comming==0)
        {

        }

        return true;
    }

    public ArrayList<String> getRead(){
        ArrayList<String> retVal=new ArrayList<String>(read.size());
        synchronized (read) {
            retVal.addAll(read);
            message_comming = 0;
            read.clear();
        }
        return retVal;
    }



    class SensordbSubThread extends Thread{

        public void run() {
            while(true) {
                try {

                    sub.jc.subscribe(new JedisPubSub() {
                        @Override
                        public void onMessage(String channel, String message) {
                            super.onMessage(channel, message);

                            synchronized (read) {

                                read.add(message);
                                message_comming = 1;
                            }
                        }
                    }, "sensorDB");

                } catch (Exception e) {
                    System.err.print("haha1");
                    System.err.print(e.toString());
                }
            }
        }
    }

}
