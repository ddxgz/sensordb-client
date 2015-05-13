package com.inesa.sensordb.api;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.gson.Gson;
import org.cesl.sensordb.client.Connection;
import org.cesl.sensordb.core.Item;
import org.cesl.sensordb.exception.DBException;

/**
 * Created by pc on 15/5/12.
 */
public class SensordbClient implements ClientInterface {
//    private String sensordb_ip;
//    private int sensordb_port;
    private Connection conn;
    private List<String> table_list;


    SensordbClient(String sensordb_ip, int sensordb_port) {
//        this.sensordb_ip = sensordb_ip;
//        this.int = sensordb_port;
        this.conn = new Connection(sensordb_ip, sensordb_port);

    }

    @Override
    public int create_table(String table_name) {
        try {
            this.conn.connect();
            this.conn.createTable(table_name);
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            System.out.println("finally create_table:[" + table_name + "]...");
            this.table_list = refresh_table_list();
            conn.close();
        }
        return 1;
    }

    @Override
    public List<String> tables() {
        this.table_list = refresh_table_list();
        return this.table_list;
    }

    private List<String> refresh_table_list() {
        List<String> table_list = new ArrayList<String>();
        try {
            this.conn.connect();
            table_list = conn.listTableNames();
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
        return table_list;
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
            this.table_list = refresh_table_list();
            conn.close();
        }
        return 1;
    }

    @Override
    public int delete_tables() {
        return 0;
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


    @Override
    public String get_json_record() {
        return null;
    }

    @Override
    public String get_records() {
        return null;
    }
}
