package com.inesa.sensordb.api.test;

import org.apache.log4j.Logger;
import org.cesl.sensordb.client.Connection;
import org.cesl.sensordb.client.ResultSet;
import org.cesl.sensordb.exception.DBException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by pc on 15-6-2.
 */
public class PerformanceTest {

    int response_time_limit;
    int row_available_time_limit;
    //1 for lowest, 4 for test for test 3 levels
    int test_pressure;
    int item_num;
    int wait_duration;

    public static String sensordb_ip = "122.144.166.103";
    public static int sensordb_port = 6677;
    public static String new_table_prefix = "ava_test_table_";
    public Date date_now = new Date();
    public long start_ltime = date_now.getTime();
    public String table_name = new_table_prefix + start_ltime;

    public static Logger logger = Logger.getLogger(AvailableTest.class);

    public PerformanceTest(){
        response_time_limit = 50;
        test_pressure = 1;
        item_num = 100;
        wait_duration = 3000;
    }

    long test_fetch(){
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String starttime = sdf.format(start_ltime-30000000);
        String endtime = sdf.format(start_ltime+600000);
//        System.out.println("st: "+starttime+" end:"+endtime);
        long time_spent = 0;
        try {
            conn.connect();
            ResultSet result_set = conn.get(table_name,
                    starttime, endtime);
            long starttimewhole = System.currentTimeMillis();
            while (result_set.next()) {}
            long endtimewhole = System.currentTimeMillis();
            logger.info("test_fetch: " + result_set.getSize() +
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
