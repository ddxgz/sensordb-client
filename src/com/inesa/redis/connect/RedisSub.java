package com.inesa.redis.connect;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shihj on 5/11/15.
 */
public class RedisSub {
    private Lock lockbuf1=new ReentrantLock();
    public final static LinkedList<String> readbuf1=new LinkedList<>();
    public final static LinkedList<String> readbuf2=new LinkedList<>();

    static String connectChannel="sensorDB";

    private static LinkedList<SensordbSubThread> sdbst;
    public boolean kill=false;

    private static volatile boolean message_comming=false;

    private JedisPool pool = null;
    private Jedis jedis = null;

    public RedisSub(String host,int port){
        try {
            pool = new JedisPool(new JedisPoolConfig(), host, port);
            jedis = pool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != jedis) {
                pool.returnBrokenResource(jedis);
                pool.destroy();
            }
        }
    }

    public boolean listen(){

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

    public LinkedList<String> getRead(){
        LinkedList<String> retVal=new LinkedList<>();
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

    class SensordbSubThread extends Thread{

        public void run() {
            while(true) {
                try {

                    jedis.subscribe(new JedisPubSub() {
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
