AppConfig = {
    'DOHKO': {
        # 'livyServerUri': 'http://172.20.44.6:8999/sessions/',
        # 'yarnServerUri': 'http://172.20.44.6:8088/ws/v1/cluster/apps/',
        # 'livyServerPath': '/usr/hdp/current/livy2-server/bin/livy-server',
        'livyServerUri': 'http://172.26.25.148:8999/sessions/',
        'yarnServerUri': 'http://172.26.25.148:8088/ws/v1/cluster/apps/',
        'livyServerPath': '/home/hadoop/livy/bin/livy-server',
        'readApp': {
            "jars": ["hdfs://cluster/yqs/tools/engine-0.0.1-SNAPSHOT.jar"],
            "pyFiles": [],
            "files": [],
            "archives": [],
            "kind": 'spark',
            "driverMemory": '2g',
            "driverCores": 1,
            "executorMemory": '2g',
            "executorCores": 2,
            "numExecutors": 4,
            "queue": 'default',
            "heartbeatTimeoutInSecond": 86400,
            "proxyUser": 'yqs',
            'conf': {
                "spark.default.parallelism": 12,
                "spark.rdd.compress": True,
                "spark.io.compression.codec": "snappy"
            }
        },
        'writeApp': {
            "jars": ["hdfs://cluster/yqs/tools/engine-0.0.1-SNAPSHOT.jar"],
            "pyFiles": [],
            "files": [],
            "archives": [],
            "kind": 'spark',
            "driverMemory": '512m',
            "driverCores": 1,
            "executorMemory": '2g',
            "executorCores": 2,
            "numExecutors": 2,
            "queue": 'default',
            "heartbeatTimeoutInSecond": 86400,
            "proxyUser": 'yqs',
            'conf': {
                "spark.default.parallelism": 12,
                "spark.rdd.compress": True,
                "spark.io.compression.codec": "snappy"
            }
        }
    },
    'PRODUCT': {
        # 'livyServerUri': 'http://rm.yqs.hualala.com:8999/sessions/',
        # 'yarnServerUri': 'http://rm.yqs.hualala.com:8088/ws/v1/cluster/apps/',
        # 'livyServerPath': '/home/olap/tools/apps/livy/bin/livy-server',
        'livyServerUri': 'http://172.26.25.148:8999/sessions/',
        'yarnServerUri': 'http://172.26.25.148:8088/ws/v1/cluster/apps/',
        'livyServerPath': '/home/hadoop/livy/bin/livy-server',
        'readApp': {
            "jars": ["hdfs://cluster/yqs/tools/engine-0.0.1-SNAPSHOT.jar"],
            "pyFiles": [],
            "files": [],
            "archives": [],
            "kind": 'spark',
            "driverMemory": '16g',
            "driverCores": 8,
            "executorMemory": '10g',
            "executorCores": 6,
            "numExecutors": 35,
            "queue": 'default',
            "heartbeatTimeoutInSecond": 86400,
            "proxyUser": None,
            'conf': {
                "spark.default.parallelism": 400,
                "spark.scheduler.mode": "FAIR",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.rdd.compress": True,
                "spark.io.compression.codec": "snappy",
                "spark.sql.inMemoryColumnarStorage.batchSize": 300000,
                "spark.sql.files.maxPartitionBytes": 134217728,
                "spark.sql.broadcastTimeout": 60,
                "spark.sql.orc.enabled": True,
                "spark.sql.orc.impl": "native",
                "spark.sql.orc.enableVectorizedReader": True,
                "spark.sql.hive.convertMetastoreOrc": True
            }
        },
        'writeApp': {
            "jars": ["hdfs://cluster/yqs/tools/engine-0.0.1-SNAPSHOT.jar"],
            "pyFiles": [],
            "files": [],
            "archives": [],
            "kind": 'spark',
            "driverMemory": '10g',
            "driverCores": 4,
            "executorMemory": '10g',
            "executorCores": 6,
            "numExecutors": 10,
            "queue": 'default',
            "heartbeatTimeoutInSecond": 86400,
            "proxyUser": None,
            'conf': {
                "spark.default.parallelism": 400,
                "spark.scheduler.mode": "FAIR",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.rdd.compress": True,
                "spark.io.compression.codec": "snappy",
                "spark.sql.inMemoryColumnarStorage.batchSize": 300000,
                "spark.sql.files.maxPartitionBytes": 134217728,
                "spark.sql.broadcastTimeout": 60,
                "spark.sql.orc.enabled": True,
                "spark.sql.orc.impl": "native",
                "spark.sql.orc.enableVectorizedReader": True,
                "spark.sql.hive.convertMetastoreOrc": True,
                "spark.sql.orc.filterPushdown": True,
                "spark.sql.orc.char.enabled": True
            }
        }
    }
}
