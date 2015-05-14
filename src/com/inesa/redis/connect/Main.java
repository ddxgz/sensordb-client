package com.inesa.redis.connect;

public class Main {
    static int max=10;
    static String addr="10.200.46.245";
    static int port=7003;
    public static RedisConnectPool mypool;
    public static void notmain(String[] args) {
	// write your code her
        //Cluster Method
        SensordbSub myssb=new SensordbSub(addr,port);
        while(myssb.listen()){
            System.err.print(myssb.getRead().toString()+"\n");
        }

        //SingleNode Method, more recommand
        /*RedisSub rs=new RedisSub(addr,port);
        while(rs.listen()){
            System.err.print(rs.getRead().toString()+"\n");
        }*/
    }

}
