#!/usr/bin/env bash
/ext/spark/bin/spark-submit --class "com.jabong.dap.init.Init" --master yarn-cluster --driver-class-path /usr/share/java/mysql-connector-java-5.1.17.jar --num-executors 3 --executor-memory 9G /opt/alchemy-core/current/jar/Alchemy-assembly-0.1.jar --component acquisition --config hdfs://dataplatform-master.jabong.com:8020/apps/alchemy/conf/config.json --tablesJson hdfs://dataplatform-master.jabong.com:8020/apps/alchemy/conf/bobAcqFull1.json

/ext/spark/bin/spark-submit --class "com.jabong.dap.init.Init" --master yarn-cluster --driver-class-path /usr/share/java/mysql-connector-java-5.1.17.jar --num-executors 3 --executor-memory 27G /opt/alchemy-core/current/jar/Alchemy-assembly-0.1.jar --component acquisition --config hdfs://dataplatform-master.jabong.com:8020/apps/alchemy/conf/config.json --tablesJson hdfs://dataplatform-master.jabong.com:8020/apps/alchemy/conf/bobAcqFull2.json

/ext/spark/bin/spark-submit --class "com.jabong.dap.init.Init" --master yarn-cluster --driver-class-path /usr/share/java/mysql-connector-java-5.1.17.jar --num-executors 3 --executor-memory 9G /opt/alchemy-core/current/jar/Alchemy-assembly-0.1.jar --component acquisition --config hdfs://dataplatform-master.jabong.com:8020/apps/alchemy/conf/config.json --tablesJson hdfs://dataplatform-master.jabong.com:8020/apps/alchemy/conf/bobAcqIncr.json

/ext/spark/bin/spark-submit --class "com.jabong.dap.init.Init" --master yarn-cluster --num-executors 3 --executor-memory 9G Alchemy-assembly-0.1.jar --component merge --config hdfs://dataplatform-master.jabong.com:8020/user/jabong/config.json --mergeJson hdfs://dataplatform-master.jabong.com:8020/user/jabong/bobMerge.json



