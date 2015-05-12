package com.inesa.sensordb.api;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Created by pc on 15-5-12.
 */
public class JsonConvertor {

    public static Map<String, byte[]> sample_values = new HashMap<String, byte[]>();
    public static Map<String, byte[]> result_map = new HashMap<String, byte[]>();
//    public static String value1 = "value1";
//    public static byte[] value1_b = value1.getBytes();
//    public static String json = "{\"name\":\"mkyong\", \"age\":\"29\"}";

    public static Date date_now = new Date();

    public static void convert() {
        try {
            String jsonstr = "{\"name\":\"mkyong\", \"age\":\"29\"}";
//            ObjectMapper mapper = new ObjectMapper();
//                sample_values = mapper.readValues(jsonstr,
//                        Map.class);

            sample_values = new Gson().fromJson(jsonstr,
                    new TypeToken<HashMap<String, Object>>() {}.getType());
            System.out.println("sample_values: " + sample_values);



        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {

            System.out.println("finally");
        }
    }
}
