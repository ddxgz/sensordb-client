package com.inesa.sensordb.api.test;

import com.inesa.sensordb.api.BasicClient;
import com.inesa.sensordb.api.SensordbItem;
import org.apache.log4j.Logger;
import org.cesl.sensordb.client.Connection;
import org.cesl.sensordb.client.ResultSet;
import org.cesl.sensordb.exception.DBException;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by pc on 15-5-14.
 */
public class AvailableTest {

    int response_time_limit;
    int row_available_time_limit;
    //1 for lowest, 4 for test for test 3 levels
    int test_pressure;
    int item_num;

    public static String sensordb_ip = "122.144.166.103";
    public static int sensordb_port = 6677;
    public static String new_table_prefix = "ava_test_table_";
    public Date date_now = new Date();
    public long start_ltime = date_now.getTime();
    public String table_name = new_table_prefix + start_ltime;

    public static Logger logger = Logger.getLogger(AvailableTest.class);

    public AvailableTest(){
        response_time_limit = 50;
        test_pressure = 1;
        item_num = 10;
    }

    AvailableTest(int resp_time, int pressure){
        response_time_limit = resp_time;
        test_pressure = pressure;
    }

    public void run_available_test() throws InterruptedException {
        test_list_tables();
        Thread.sleep(5000);

        test_create_table();
        Thread.sleep(5000);

//        test_list_tables();
//        Thread.sleep(1000);

        test_put();
        Thread.sleep(6000);

        test_get();
        Thread.sleep(5000);

        test_drop_table();
        Thread.sleep(5000);

//        test_list_tables();
    }

    long test_list_tables(){
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        List<String> table_list = new ArrayList<String>();
        long time_spent = 0;
        try {
            long starttimewhole = System.currentTimeMillis();
            conn.connect();
            table_list = conn.listTableNames();
            long endtimewhole = System.currentTimeMillis();
            logger.info("test_list_tables: " + table_list.size() +
                    " tables in 1 conn: "
                    + (endtimewhole - starttimewhole) + " ms");
            time_spent = endtimewhole - starttimewhole;
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
        return time_spent;
    }

    void test_create_table(){
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        try {
            long starttimewhole = System.currentTimeMillis();
            conn.connect();
            conn.createTable(table_name);
            long endtimewhole = System.currentTimeMillis();
            logger.info("test_create_table: " + table_name +
                    " in 1 conn: "
                    + (endtimewhole - starttimewhole) + " ms");
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
    }


    void test_drop_table(){
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        try {
            long starttimewhole = System.currentTimeMillis();
            conn.connect();
            conn.dropTable(table_name);
            long endtimewhole = System.currentTimeMillis();
            logger.info("test_drop_table: " + table_name +
                    " in 1 conn: "
                    + (endtimewhole - starttimewhole) + " ms");
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
    }


    long test_put(){
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        List<String> table_list = new ArrayList<String>();
        Utils utils = new Utils();
        long time_spent = 0;
        try {
            List<SensordbItem> items = utils.get_sensordb_items(item_num);
            long starttimewhole = System.currentTimeMillis();
            conn.connect();
            for (SensordbItem item : items){
                conn.put(table_name, item.sensorID,
                        item.timestamp,
                        item.x, item.y, item.z, item.values);
            }
            long endtimewhole = System.currentTimeMillis();
            logger.info("test_put: " + items.size() +
                    " items in 1 conn: "
                    + (endtimewhole - starttimewhole) + " ms");
            time_spent = endtimewhole - starttimewhole;
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
        return time_spent;
    }

    long test_get(){
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String starttime = sdf.format(start_ltime-30000000);
        String endtime = sdf.format(start_ltime+300000);
//        System.out.println("st: "+starttime+" end:"+endtime);
        long time_spent = 0;
        try {
            long starttimewhole = System.currentTimeMillis();
            conn.connect();
            ResultSet result_set = conn.get(table_name,
                    starttime, endtime);
            long endtimewhole = System.currentTimeMillis();
            logger.info("test_get: " + result_set.getSize() +
                    " items in 1 conn: "
                    + (endtimewhole - starttimewhole) + " ms");
            time_spent = endtimewhole - starttimewhole;
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
        return time_spent;
    }
}
