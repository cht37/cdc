package com.neu.monitorSys.grid;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class GridCoverage {
    public static void main(String[] args) {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置并行度
        env.setParallelism(1);

        try {
            // 定义 MySQL 源表
            String gridArea = """
                CREATE TABLE grid_manager_area (
                    id INT,
                    district_id STRING,
                    area_name STRING,
                    PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                    'connector' = 'mysql-cdc',
                    'hostname' = '192.168.244.128',
                    'port' = '3306',
                    'username' = 'root',
                    'password' = 'cht021125',
                    'database-name' = 'monitor_sys',
                    'table-name' = 'grid_manager_area',
                    'server-time-zone' = 'Asia/Shanghai'
                );
            """;
            tableEnv.executeSql(gridArea);

            String district = """
                CREATE TABLE districts (
                    id INT,
                    district_id STRING,
                    district_name STRING,
                    city_id STRING,
                    PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                    'connector' = 'mysql-cdc',
                    'hostname' = '192.168.244.128',
                    'port' = '3306',
                    'username' = 'root',
                    'password' = 'cht021125',
                    'database-name' = 'monitor_sys',
                    'table-name' = 'districts',
                    'server-time-zone' = 'Asia/Shanghai'
                );
            """;
            tableEnv.executeSql(district);

            // 定义 Elasticsearch Sink 表
            String sinkEs = """
                CREATE TABLE es_sink (
                    id STRING,
                    coverage_rate DECIMAL(10, 4),
                    PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                    'connector' = 'elasticsearch-7',
                    'hosts' = 'http://192.168.244.128:9200',
                    'index' = 'grid_coverage',
                    'format' = 'json',
                    'json.timestamp-format.standard' = 'ISO-8601'
                );
            """;
            tableEnv.executeSql(sinkEs);

            // 定义查询并插入到 Elasticsearch
            String query = """
                INSERT INTO es_sink
                SELECT
                    '1' AS id,
                    CAST((COUNT(DISTINCT d.district_id) / (SELECT CAST(COUNT(*) AS DECIMAL(10, 4)) FROM districts)) * 100 AS DECIMAL(10, 2)) AS coverage_rate
                FROM
                    districts d
                    LEFT JOIN grid_manager_area g ON d.district_id = g.district_id
                WHERE
                    g.id IS NOT NULL;
            """;
            TableResult result = tableEnv.executeSql(query);
            result.print();  // 打印执行结果，便于调试

        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            // 执行 Flink 作业
            env.execute("grid_coverage");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
