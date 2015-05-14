package com.inesa.redis.connect;

import redis.clients.jedis.JedisPubSub;

import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shihj on 5/11/15.
 */
public class SensordbSub {
    public static RedisConnectPool mypool;

    private Lock lockbuf1=new ReentrantLock();
    public final static LinkedList<String> readbuf1=new LinkedList<>();
    public final static LinkedList<String> readbuf2=new LinkedList<>();

    static int max=10;
    static String addr="10.200.46.245";
    static int port=7000;
    static String connectChannel="sensorDB";

    private RedisClusterSub sub;
    private LinkedList<SensordbSubThread> sdbst;
    public boolean kill=false;

    private static volatile boolean message_comming=false;

    public SensordbSub(String redisAddr, int redisPort){
        max=1;
        addr=redisAddr;
        port=redisPort;
        mypool=new RedisConnectPool(1,addr,port);
        if (mypool.createPool()==0)
            System.err.print("CreatePool failed");
        else
            sub= mypool.getRedisCLusterSub();
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
            sdbst=new LinkedList<>();
            SensordbSubThread temp1=new SensordbSubThread();
            sdbst.add(temp1);
            temp1.start();
        }

        while(!message_comming)
        {

        }

        return true;
    }

    public int destroy(){
        if(sdbst.size()>0)
        {
            kill=true;
        }
        sdbst.clear();
        while(!lockbuf1.tryLock()){
            readbuf1.clear();
            lockbuf1.unlock();
        }
        readbuf2.clear();
        return 1;
    }

    public LinkedList<String> getRead(){
        LinkedList<String> retVal = new LinkedList<>();
        if (lockbuf1.tryLock())
        {
            retVal.addAll(readbuf1);
            readbuf1.clear();
            lockbuf1.unlock();
        }else{
            retVal.addAll(readbuf2);
            readbuf2.clear();
        }
        message_comming = false;
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

                            if (lockbuf1.tryLock())
                            {
                                message_comming = true;
                                readbuf1.add(message);
                                lockbuf1.unlock();
                            }else{
                                message_comming = true;
                                readbuf2.add(message);
                            }
                        }
                    }, connectChannel);

                } catch (Exception e) {
                    System.err.print(e.toString());
                    break;
                }
                if (kill)
                {
                    break;
                }
            }
        }
    }

}
