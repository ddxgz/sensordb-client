/**
 * Created by pc on 15-5-11.
 */

//import java.io.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cesl.sensordb.client.Connection;
import org.cesl.sensordb.client.ResultSet;
import org.cesl.sensordb.core.Item;
import org.cesl.sensordb.exception.DBException;


public class BasicClient {
    public static String sensordb_ip = "122.144.166.103";
    public static int sensordb_post = 6677;
    public static List<String> table_list;
    public static List<String> table_list_t2;
    public static boolean table_t2_exist;
    public static String table_test_name = "table_test1";
    public static int status;

    public static String sensorID = "sensor1";
    public static long sample_timestamp = 1431314011;
    public static double[] spacexyz = {1.2, 2.3, 3.4};
    public static Map<String, byte[]> sample_values = new HashMap();
//    public static String value1 = "value1";
//    public static byte[] value1_b = value1.getBytes();

    public static Item item1 = new Item();

    public static ResultSet result_set;


    public static void main(String[] args) {
        Connection conn = new Connection(sensordb_ip, sensordb_post);
        try {
            conn.connect();
            System.out.println("Connecting Done.");
            table_list = conn.listTableNames();
            System.out.println("table_list: " + table_list);
            table_list_t2 = conn.listTableNames();
            System.out.println("listTableNames(): " + table_list_t2);
            table_list_t2 = conn.listTableNames("test1$");
            System.out.println("listTableNames(\"test1$\"): "
                    + table_list_t2);
            table_t2_exist = conn.isExisted("table_test1");
            System.out.println("isExisted(\"table_test1\"): " + table_t2_exist);
//            conn.createTable(table_test_name+"22");
//            System.out.println("createTable: " + table_test_name);

//            conn.dropTables("/12$");
//              conn.dropTable(table_test_name+"12");

//            System.out.println("dropTables: ");

//            sample_values.put("key3", "value3".getBytes());
//            sample_values.put("key4", "value4".getBytes());
//            status = conn.put(table_test_name, sensorID.getBytes(),
//                    sample_timestamp,
//                spacexyz[0], spacexyz[1], spacexyz[2], sample_values);
//            System.out.println("table: " + table_test_name + " -put status: "
//                    + status);

            result_set = conn.get("t2", sensorID.getBytes(),
                    "47320-01-01 01:00:00",
                    "47337-12-30 23:00:00");
            System.out.println("table: " + table_test_name + " -get result: "
                    + result_set + " error code: " + result_set.getErrCode()
                    + " size: " + result_set.getSize());
            for (int i = 0; i < result_set.getSize(); ++i) {
//                result_map = result_set.get.getRows(0,2);
                System.out.println("table: " + table_test_name
                        + " -get result next:" + result_set.toString());
            }


        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
//            conn.dropTable(table_test_name);
//            System.out.println("dropTable: " + table_test_name);
            conn.close();
        }
    }
}
