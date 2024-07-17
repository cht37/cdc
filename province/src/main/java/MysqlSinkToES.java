import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/*
https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/elasticsearch/
 */
public class MysqlSinkToES {
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
            String StatisticsSource = """
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
            tableEnv.executeSql(StatisticsSource);
            String sinkEs = """
                    CREATE TABLE statistics_province (
                      province_id STRING,
                      avg_so2_value DOUBLE,
                      avg_co_value DOUBLE,
                      avg_spm_value DOUBLE,
                      avg_aqi DOUBLE
                    ) WITH (
                      'connector' = 'elasticsearch-7',
                      'hosts' = 'http://192.168.244.128:9200',
                      'index' = 'statistics_province',
                      'sink.bulk-flush.max-actions' = '1',
                      'format' = 'json',
                      'json.timestamp-format.standard' = 'ISO-8601'
                    );
                    """;
            tableEnv.executeSql(sinkEs);
            String query = """
                    INSERT INTO statistics_province
                    SELECT
                        province_id,
                        AVG(so2_value) AS avg_so2_value,
                        AVG(co_value) AS avg_co_value,
                        AVG(spm_value) AS avg_spm_value,
                        AVG(aqi) AS avg_aqi
                    FROM
                        statistics
                    WHERE
                         CAST(confirm_datetime AS DATE) >= CURRENT_DATE - INTERVAL '7' DAY
                      AND CAST(confirm_datetime AS DATE) < CURRENT_DATE + INTERVAL '1' DAY
                    GROUP BY
                        province_id              
                    """;
            tableEnv.executeSql(query);

            env.execute("MysqlSinkToES: statistics to statistics_es");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}

