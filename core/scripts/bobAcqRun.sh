/ext/spark/bin/spark-submit --class "com.jabong.dap.init.Init" --master yarn-cluster --driver-class-path /usr/share/java/mysql-connector-java-5.1.17.jar --num-executors 3 --executor-memory 3G /opt/alchemy-core/current/jar/Alchemy-assembly-0.1.jar --component acquisition --config hdfs://dataplatform-master.jabong.com:8020/apps/alchemy/conf/config.json --tablesJson hdfs://dataplatform-master.jabong.com:8020/apps/alchemy/conf/bobAcquisitionTables.json
