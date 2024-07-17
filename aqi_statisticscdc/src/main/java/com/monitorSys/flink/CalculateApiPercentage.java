package com.monitorSys.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CalculateApiPercentage {
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        try {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            //checkpoint配置
//            env.enableCheckpointing(5000);
            //设置并行度
            env.setParallelism(1);
            String statisticsSource = """
                    CREATE TABLE statistics (
                      id INT,
                      af_id INT,
                      province_id STRING,
                      city_id STRING,
                      district_id STRING,
                      address STRING,
                      so2_value INT,
                      so2_aqi INT,
                      co_value INT,
                      co_aqi INT,
                      spm_value INT,
                      spm_aqi INT,
                      aqi INT,
                      confirm_datetime TIMESTAMP,
                      gm_id STRING,
                      fd_tel STRING,
                      information STRING,
                      remarks STRING,
                      PRIMARY KEY (id, af_id) NOT ENFORCED
                    )  WITH(
                         'connector' = 'mysql-cdc',
                         'hostname' = '192.168.244.128',
                         'port' = '3306',
                         'username' = 'root',
                          'password' = 'cht021125',
                          'database-name' = 'monitor_sys',
                    	  'table-name' = 'statistics',
                        'server-time-zone' = 'Asia/Shanghai'
                    );
                    """;
            tableEnv.executeSql(statisticsSource);
            String aqiSource = """
                    CREATE TABLE aqi (
                      aqi_id INT,
                      chinese_explain STRING,
                      aqi_explain STRING,
                      aqi_min INT,
                      aqi_max INT,
                      color STRING,
                      health_impact STRING,
                      take_steps STRING,
                      so2_min INT,
                      so2_max INT,
                      co_min INT,
                      co_max INT,
                      spm_min INT,
                      spm_max INT,
                      remarks STRING,
                      PRIMARY KEY (aqi_id) NOT ENFORCED
                    ) WITH (
                      'connector' = 'mysql-cdc',
                      'hostname' = '192.168.244.128',
                      'port' = '3306',
                      'username' = 'root',
                      'password' = 'cht021125',
                      'database-name' = 'monitor_sys',
                      'table-name' = 'aqi',
                      'server-time-zone' = 'Asia/Shanghai'
                    );
                    """;
            tableEnv.executeSql(aqiSource);
            String aqiPercentageSink = """
                     CREATE TABLE aqi_statistics (
                       aqi_id INT,
                       chinese_explain STRING,
                       aqi_count BIGINT,
                       aqi_percentage DOUBLE,
                       PRIMARY KEY (aqi_id) NOT ENFORCED
                     ) WITH (
                       'connector' = 'elasticsearch-7',
                       'hosts' = 'http://192.168.244.128:9200',
                       'index' = 'aqi_statistics'
                     );
                    """;
            tableEnv.executeSql(aqiPercentageSink);
            String aqiPercentage = """
                    INSERT INTO aqi_statistics
                    SELECT
                      aqi.aqi_id,
                      aqi.chinese_explain,
                      COUNT(statistics.id) AS aqi_count,
                      COUNT(statistics.id) * 100.0 / (SELECT COUNT(*) FROM statistics) AS aqi_percentage
                    FROM
                      statistics,
                      aqi
                    WHERE
                      (statistics.aqi >= aqi.aqi_min AND statistics.aqi <= aqi.aqi_max)
                    GROUP BY
                      aqi.aqi_id,
                      aqi.chinese_explain;
                    """;
            tableEnv.executeSql(aqiPercentage);
            env.execute("calculateApiPercentage");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
