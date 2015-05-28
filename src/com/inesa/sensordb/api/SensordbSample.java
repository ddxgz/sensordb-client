package com.inesa.sensordb.api;

import org.cesl.sensordb.client.Connection;
import org.cesl.sensordb.client.ResultSet;
import org.cesl.sensordb.core.Item;
import org.cesl.sensordb.exception.DBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


public class SensordbSample {
    public static String sensordb_ip = "122.144.166.103";
    public static int sensordb_port = 6677;
    public static String new_table = "test_table_1";
    public static String sensorID = "camera1";
    public static Date date_now = new Date();
    public static double[] spacexyz = {1.2, 2.3, 3.4};
    public static Map<String, byte[]> sample_values = new HashMap();


    public void create_table() {
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        try {
            conn.connect();
            conn.createTable("test_table_2");
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
    }


    public void delete_table() {
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        try {
            conn.connect();
            conn.dropTable("test_table_2");
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
    }


    public void list_tables() {
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        try {
            conn.connect();
            List<String> table_list = conn.listTableNames();
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
    }


    public void put() {
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        try {
            conn.connect();
            sample_values.put("key1", "value1".getBytes());
            sample_values.put("key2", "value2".getBytes());
            conn.put(new_table, sensorID.getBytes(), date_now.getTime(),
                    spacexyz[0], spacexyz[1], spacexyz[2], sample_values);
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
    }


    public void get() {
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        try {
            conn.connect();

            ResultSet result_set = conn.get(new_table, /*optionally add params*/
                    "2015-05-12 06:29:43", "2015-05-12 12:50:00");
            System.out.println("table: " + new_table
                    + " --error code: " + result_set.getErrCode()
                    + " --size: " + result_set.getSize());
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
    }


    public void multi_put(int item_num) {
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        try {
            conn.connect();
            List<Item> items = get_items_for_batchPut(item_num);
            conn.batchPut(new_table, items);

        } catch (DBException e1) {
            e1.printStackTrace();
        } finally {
            conn.close();
        }
    }


    public List<Item> get_items_for_batchPut(int num) {
        Date date = new Date();
        Item item = new Item();
        List<Item> items = new ArrayList<Item>();
        for (int i = 0; i < num; ++i) {
            Map<String, ByteBuffer> values = new HashMap();
            values.put("key_test1" + String.valueOf(i),
                    ByteBuffer.wrap(("value_" + String.valueOf(i)).getBytes()));
            item.sensorID = ByteBuffer.wrap(sensorID.concat("_m_")
                    .concat(String.valueOf(i)).getBytes());
            item.sampledts = date.getTime();
            item.x = spacexyz[0]+i;
            item.y = spacexyz[1]+i;
            item.z = spacexyz[2]+i;
            item.values = values;
            items.add(item);
        }
        return items;
    }


    public void put_from_jsonstr(String json_value) {
        Connection conn = new Connection(sensordb_ip, sensordb_port);
        SensordbItem item = new SensordbItem(json_value);

        try {
            conn.connect();
            conn.put(new_table, item.sensorID, item.timestamp,
                    item.x, item.y, item.z, item.values);
        } catch (DBException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.close();
        }
    }


    class SensordbItem {
        public byte[] sensorID;
        public long timestamp;
        public double x;
        public double y;
        public double z;
        public Map<String, byte[]> values = new HashMap<String, byte[]>();

        private ItemWithoutValue item_without_value = new ItemWithoutValue();
        private String values_str;
        private Map<String, Object> values_objmap = new HashMap<String, Object>();

        SensordbItem() {}

        SensordbItem(String json_in) {
            set_item(json_in);
        }

        public void set_item(String json_in) {
            JsonConvertor jsonconv = new JsonConvertor();
            try {
                this.item_without_value = jsonconv.item_without_value(json_in);
                values_str = jsonconv.sub_json(json_in, "values");
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.sensorID = this.item_without_value.sensorID.getBytes();
            this.timestamp = this.item_without_value.timestamp;
            this.x = this.item_without_value.x;
            this.y = this.item_without_value.y;
            this.z = this.item_without_value.z;
            values_objmap = jsonconv.get_objmap(values_str);

            for (Map.Entry<String, Object> entry : values_objmap.entrySet()) {
                this.values.put(entry.getKey(),
                        entry.getValue().toString().getBytes());
            }
        }
    }
}
