import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Initialize Flink Table Environment (Streaming Mode)
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# 2. Inject the downloaded JARs directly into Flink's classpath
current_dir = os.path.abspath(os.getcwd())
jar_paths = ";".join(
    [
        f"file://{current_dir}/flink-sql-connector-kafka-3.0.2-1.18.jar",
        f"file://{current_dir}/flink-connector-jdbc-3.1.2-1.18.jar",
        f"file://{current_dir}/mysql-connector-j-8.0.33.jar",
    ]
)
t_env.get_config().set("pipeline.jars", jar_paths)

# 3. Define the Kafka Source Stream
# PROCTIME() automatically tags each event with the exact time Flink reads it
t_env.execute_sql("""
    CREATE TABLE kafka_source (
        user_id STRING,
        plan STRING,
        action STRING,
        proc_time AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'subscriptions',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'flink_group',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
""")

# 4. Define the MySQL Sink
t_env.execute_sql("""
    CREATE TABLE mysql_sink (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        plan STRING,
        action STRING,
        total_revenue DOUBLE,
        event_count BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://localhost:3306/streaming_db',
        'table-name' = 'subscription_metrics_flink',
        'username' = 'admin',
        'password' = 'password',
        'driver' = 'com.mysql.cj.jdbc.Driver'
    )
""")

# 5. Execute the Stateful Streaming Query
# We use a 1-Minute TUMBLE window to aggregate the streaming data
query = """
    INSERT INTO mysql_sink
    SELECT
        TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
        TUMBLE_END(proc_time, INTERVAL '1' MINUTE) AS window_end,
        plan,
        action,
        SUM(
            CASE
                WHEN action = 'buy' AND plan = 'Basic' THEN 149.0
                WHEN action = 'buy' AND plan = 'HD' THEN 199.0
                WHEN action = 'buy' AND plan = 'FullHD' THEN 499.0
                WHEN action = 'buy' AND plan = '4K' THEN 649.0
                ELSE 0.0
            END
        ) AS total_revenue,
        COUNT(*) AS event_count
    FROM kafka_source
    GROUP BY
        TUMBLE(proc_time, INTERVAL '1' MINUTE),
        plan,
        action
"""

print("Starting Flink Stateful Streaming Job...")
t_env.execute_sql(query).wait()
