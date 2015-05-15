package com.inesa.sensordb.api;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.Gson;

import org.apache.log4j.Logger;
import org.cesl.sensordb.client.Connection;
import org.cesl.sensordb.client.ResultSet;
//import org.cesl.sensordb.core.ResultSet;
import org.cesl.sensordb.exception.DBException;

/**
 * Created by pc on 15/5/12.
 */
public class SensordbClient implements ClientInterface {
    //    private String sensordb_ip;
//    private int sensordb_port;
    public Connection conn;
    private List<String> table_list;

    public static Logger logger = Logger.getLogger(SensordbClient.class);


    SensordbClient(Connection conn) {
//        this.sensordb_ip = sensordb_ip;
//        this.int = sensordb_port;
        this.conn = conn;

    }

    SensordbClient(String sensordb_ip, int sensordb_port) {
//        this.sensordb_ip = sensordb_ip;
//        this.int = sensordb_port;
        this.conn = new Connection(sensordb_ip, sensordb_port);

    }

    public void connect() throws DBException {
        this.conn.connect();
    }

    public void close() throws DBException {
        this.conn.close();
    }
//    @Override
//    public void close() throws Exception{
//        System.out.println("close()...");
//    }
//
//    @Override
//    protected void finalize() throws Throwable {
//        super.finalize();
//    }

    @Override
    public int create_table(String table_name) {
        try {
            this.conn.connect();
            this.conn.createTable(table_name);
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
//            return 0;
        } finally {
            System.out.println("finally create_table:[" + table_name + "]...");
            refresh_table_list();
            conn.close();
        }
        return 1;
    }

    @Override
    public List<String> tables() {
        refresh_table_list();
        return this.table_list;
    }

    private void refresh_table_list() {
        List<String> table_list = new ArrayList<String>();
        try {
            this.conn.connect();
            this.table_list = conn.listTableNames();
//            System.out.println("table_list: " + table_list);
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
            System.out.println("finally refresh_table_list...");
            conn.close();
        }
//        return table_list;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public int delete_table(String table_name) {
        try {
            this.conn.connect();
            this.conn.dropTable(table_name);
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            System.out.println("finally delete_table: [" + table_name + "] ...");
            refresh_table_list();
            conn.close();
        }
        return 1;
    }

    @Override
    public int delete_tables() {
        return 0;
    }

    @Override
    public int long_put(String table_name, List<String> json_str_list) {
//        Map<String, byte[]> values_map = new HashMap<String, byte[]>();
//        JsonConvertor jsonconv = new JsonConvertor();
        int status = 0;
//        SensordbItem item = new SensordbItem(json_value);
        this.used.set(1);

        if (connected.intValue() == 0) {
            try {
                this.conn.connect();
                this.connected.set(1);
    //            this.used.set(1);
                connected_num.addAndGet(1);
                logger.info("long put connect(), set used to 1, " +
                        "connected_num-closed_num: " + connected_num + "-"
                        + closed_num);
            } catch (DBException e) {
                e.printStackTrace();
            }
        }
//        this.conn.connect();

        for (String str_in : json_str_list){
            System.out.println("redis str_in: " + str_in);
            //     put_sensordb(str_in);
            SensordbItem item = new SensordbItem(str_in);
            conn.put(table_name, item.sensorID,
                    item.timestamp,
                    item.x, item.y, item.z, item.values);
//            ++receive_cnt;
        }
        this.used.set(0);
        logger.info("long put set used to 0");
        return status;
    }


    public int long_put_items(String table_name, List<SensordbItem> item_list)
            throws DBException {
//        Map<String, byte[]> values_map = new HashMap<String, byte[]>();
//        JsonConvertor jsonconv = new JsonConvertor();
        int status = 0;
//        SensordbItem item = new SensordbItem(json_value);
        this.used.set(1);

        if (connected.intValue() == 0) {
            this.conn.connect();
            this.connected.set(1);
//            this.used.set(1);
            connected_num.addAndGet(1);
            logger.info("long put connect(), set used to 1, " +
                    "connected_num-closed_num: "+connected_num+"-"+closed_num);
        }
//        this.conn.connect();

        for (SensordbItem item : item_list){
//            System.out.println("redis str_in: " + str_in);
            //     put_sensordb(str_in);
//            SensordbItem item = new SensordbItem(str_in);
            conn.put(table_name, item.sensorID,
                    item.timestamp,
                    item.x, item.y, item.z, item.values);
//            ++receive_cnt;
        }
//        this.used.set(0);
//        logger.info("long put set used to 0");
        return status;
    }


    @Override
    public int put_record(String table_name, String json_value) {
//        Map<String, byte[]> values_map = new HashMap<String, byte[]>();
//        JsonConvertor jsonconv = new JsonConvertor();
        int status = 0;
        SensordbItem item = new SensordbItem(json_value);

        try {
            this.conn.connect();
            status = conn.put(table_name, item.sensorID, item.timestamp,
                    item.x, item.y, item.z, item.values);
//            System.out.println("table: " + table_test_name + " -put status: "
//                    + status);

        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
//            System.out.println("finally");
            conn.close();
        }
        return status;
    }


    //TODO
    public int put_record_safe(String table_name, String json_value) {
//        Map<String, byte[]> values_map = new HashMap<String, byte[]>();
//        JsonConvertor jsonconv = new JsonConvertor();
        int status = 0;
        SensordbItem item = new SensordbItem(json_value);

        int put_status = put_record(table_name, json_value);

        try {
            this.conn.connect();
//            status = conn.get(table_name, item.sensorID, item.timestamp,
//                    item.x, item.y, item.z, item.values);
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
//            System.out.println("finally");
            conn.close();
        }
        return status;
    }

    @Override
    public int put_records(String table_name) {
        return 0;
    }

    @Override
    public int delete_record() {
        return 0;
    }

    @Override
    public int delete_records() {
        return 0;
    }


    class ResultItem {
        String id;
        long ts;
        double x;
        double y;
        double z;
        Map<String, String> values = new HashMap<>();
    }

    @Override
    public List<String> get_json_record(String table_name, String sensorID,
                                  String starttime, String endtime,
                                  List<String> valuekeys) {
        String jsonstr = " ";
        List<String> json_list = new ArrayList<>();
        Gson gson = new Gson();
//        SensordbItem item = new SensordbItem();
        ResultItem item = new ResultItem();
//        ResultSet result_set = new ResultSet();
        try {
            this.conn.connect();
            ResultSet result_set = this.conn.get(table_name,
                    sensorID.getBytes(), starttime, endtime);
            System.out.println("result_set: " + result_set + " size:"
                    + result_set.getSize() + " errorcode:" +
                    result_set.getErrCode());

            while (result_set.next()) {
                result_set.getString("id");
                item.id = result_set.getString("id");
                item.ts = result_set.getLong("ts");
                item.x = result_set.getDouble("x");
                item.y = result_set.getDouble("y");
                item.z = result_set.getDouble("z");
//                String tst = result_set.getString("word_separators");
                for (int i=0; i<valuekeys.size(); ++i){
                    item.values.put(valuekeys.get(i),
                            result_set.getString(valuekeys.get(i)));
                }
                System.out.println("item- id:" + item.id + " - ts:" + item.ts/*+
                        " tst:"+tst+" x:"+item.x+" y:"+item.y+" z:"+item.z*/
                        +"values: "+item.values);
                jsonstr = gson.toJson(item);
                json_list.add(jsonstr);
            }

        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            System.out.println("get_record finally");
            conn.close();
        }

        return json_list;
    }


    @Override
    public String get_records() {
        return null;
    }


    private AtomicInteger connected = new AtomicInteger(0);
    private AtomicInteger used = new AtomicInteger(0);
    private AtomicInteger connected_num = new AtomicInteger(0);
    private AtomicInteger closed_num = new AtomicInteger(0);
    private InputStream inStream;
    private OutputStream outStream;
    private Process process;
    private boolean abortCondition = false;
    private int watchDogTSleepTime = 10000; //3 sek
    private int timer = 0;
    public void startWatchDog(final Connection conn) {
        Runnable r = new Runnable() {

            @Override
            public void run() {
                while(!abortCondition){
                    try {

                        Thread.sleep(watchDogTSleepTime);
                        if (connected.intValue() == 0) {
                            conn.connect();
                            connected.set(1);
                            connected_num.addAndGet(1);
                            logger.info("watchdog connect(), "+ "connected_num" +
                                    "-closed_num: "+connected_num+"-"+closed_num);
                        }
                        if (used.intValue() == 1) {
                            used.set(0);
                            Thread.sleep(watchDogTSleepTime);
//                            ++timer;
//                            logger.info("timer: "+timer);

                            if (used.intValue() == 0 & connected.intValue() == 1) {
                                connected.set(0);
                                conn.close();
                                closed_num.addAndGet(1);
                                logger.info("watchdog close(), " +
                                        "connected_num-closed_num: " + connected_num
                                        + "-" + closed_num);
                            }
                        }
//                        Thread.sleep(watchDogTSleepTime);
//                        if (timer>5) {
//                            if (used.intValue() == 0 & connected.intValue() == 1) {
//                                connected.set(0);
//                                conn.close();
//                                closed_num.addAndGet(1);
//                                logger.info("watchdog close(), timer > 10 " +
//                                        "connected_num-closed_num: " + connected_num
//                                        + "-" + closed_num);
//                                timer = 0;
//                            }
//
//                        }


                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (DBException e) {
                        e.printStackTrace();
                    }

                    //if you want, you might try to kill the process
//                    process.destroy();
                }
            }
        };

        Thread watchDog = new Thread(r);
        watchDog.start();
        //watchDog.setDaemon(true); //maybe
    }
}
