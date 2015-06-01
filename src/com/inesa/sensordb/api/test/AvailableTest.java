package com.inesa.sensordb.api.test;

/**
 * Created by pc on 15-5-14.
 */
public class AvailableTest {

    int response_time_limit;
    //1 for lowest, 4 for test for test 3 levels
    int test_pressure;

    AvailableTest(){
        response_time_limit = 50;
        test_pressure = 1;
    }

    AvailableTest(int resp_time, int pressure){
        response_time_limit = resp_time;
        test_pressure = pressure;
    }

    public void run_available_test(){};

    void test_list_tables(){};
    void test_create_table(){};
    void test_drop_table(){};
    void test_put(){};
    void test_get(){};
}
