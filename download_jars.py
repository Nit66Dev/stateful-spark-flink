import urllib.request
import ssl
import os

# Disable SSL verification to bypass corporate VPN filters
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Dictionary of JAR names and their Maven download URLs
jars = {
    "flink-sql-connector-kafka-3.0.2-1.18.jar": "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar",
    "flink-connector-jdbc-3.1.2-1.18.jar": "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar",
    "mysql-connector-j-8.0.33.jar": "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar",
}

print("Starting JAR downloads...")

for name, url in jars.items():
    if not os.path.exists(name):
        print(f"Downloading {name}...")
        try:
            # We use urlopen with the context bypass, then write the file manually
            with urllib.request.urlopen(url, context=ctx) as response:
                with open(name, "wb") as out_file:
                    out_file.write(response.read())
            print(f"Successfully downloaded {name}!")
        except Exception as e:
            print(f"Failed to download {name}. Error: {e}")
    else:
        print(f"{name} already exists. Skipping.")

print("All downloads complete!")
