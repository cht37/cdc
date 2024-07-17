package com.monitorSys.flink;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/*
https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/elasticsearch/
 */
public class MysqlSinkToEsFeedback {
    public static void main(String[] args) throws Exception {
         // 设置执行环境
        try {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            //设置并行度
            env.setParallelism(1);

//        //checkpoint配置
//        env.enableCheckpointing(5000);

            //MySQLsource
            String AqiFeedbackSource ="""
                           CREATE TABLE IF NOT EXISTS  aqi_feedback (
                                af_id  INT,
                                tel_id STRING ,
                                province_id STRING ,
                                city_id STRING,
                                district_id STRING,
                                address STRING,
                                information STRING,
                                estimated_grade INT,
                                af_date DATE,
                                af_time TIME,
                                gm_id STRING,
                                assign_date DATE,
                                assign_time TIME,
                                state INT,
                                confirm_datetime TIMESTAMP,
                                remarks STRING,
                                PRIMARY KEY (af_id) NOT ENFORCED
                              ) WITH(
                                   'connector' = 'mysql-cdc',
                                   'hostname' = '192.168.244.128',
                                   'port' = '3306',
                                   'username' = 'root',
                                    'password' = 'cht021125',
                                    'database-name' = 'monitor_sys',
                              	  'table-name' = 'aqi_feedback',
                                  'server-time-zone' = 'Asia/Shanghai'
                              );
                    """;
            tableEnv.executeSql(AqiFeedbackSource);
            String sinkEs = """
                    CREATE TABLE IF NOT EXISTS  aqi_feedback_es (
                            af_id  INT,
                            tel_id STRING ,
                            province_id STRING ,
                            city_id STRING,
                            district_id STRING,
                            address STRING,
                            information STRING,
                            estimated_grade INT,
                            af_date DATE,
                            af_time TIME,
                            gm_id STRING,
                            assign_date DATE,
                            assign_time TIME,
                            state INT,
                             confirm_datetime TIMESTAMP(3),
                            remarks STRING,
                            PRIMARY KEY (af_id) NOT ENFORCED
                          ) WITH(
                          'connector'= 'elasticsearch-7',
                           'hosts' = 'http://192.168.244.128:9200',
                           'index' = 'aqi_feedback_es',
                           'sink.bulk-flush.max-actions' = '1',
                            'format' = 'json',
                            'json.timestamp-format.standard' = 'ISO-8601'
                          );
                    """;
            tableEnv.executeSql(sinkEs);
            String query = "INSERT INTO aqi_feedback_es SELECT * FROM aqi_feedback";
            TableResult result = tableEnv.executeSql(query);

            env.execute("MysqlSinkToES: aqi_feedback to aqi_feedback_es");
        } catch (Exception e) {
             e.printStackTrace();
        }


    }


}

