package com.inesa.redis.connect;

public class Main {
    static int max=10;
    static String addr="10.200.46.245";
    static int port=7003;
    public static RedisConnectPool mypool;
    public static void notmain(String[] args) {
	// write your code her
        /*mypool=new RedisConnectPool(1,addr,port);
        mypool.createPool();

        new Thread(new Runnable() {
            public void run() {
                try {
                    RedisClusterSub sub= mypool.getRedisCLusterSub();
                    sub.jc.subscribe(new JedisPubSub() {
                        @Override
                        public void onMessage(String channel, String message) {
                            super.onMessage(channel, message);
                            System.err.print(message);
                        }
                    }, "sensorDB");

                } catch (Exception e) {
                    System.err.print(e.toString());
                }
            }
        }).start();*/


        SensordbSub myssb=new SensordbSub(addr,port);
        while(myssb.listen()){
            System.err.print(myssb.getRead().toString());
        }
    }

}
