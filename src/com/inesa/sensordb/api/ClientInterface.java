package com.inesa.sensordb.api;

import java.util.List;

/**
 * Created by pc on 15/5/12.
 */
public interface ClientInterface {

    int create_table(String table_name);

    List<String> tables();

    //number of tables
    long size();

    //could return the table name
    int delete_table(String table_name);

    int delete_tables();

    interface Table {

        long size();

        int put_record();

        int put_records();
    }

    int put_record(String table_name, String json_value);

    int put_records(String table_name);

    int delete_record();

    int delete_records();

    //return a json string
    String get_json_record();

    String get_records();
}
