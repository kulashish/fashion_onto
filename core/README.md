# Alchemy
 DAP (Data & Analytics Platform) repository

### Compile, Test, and Coverage

Compile and create assembly jar (i.e., jar with all dependencies):

        $ sbt clean assembly

Run Unit Test

        $ sbt test

Run Code Coverage

        $ sbt clean coverage test
        $ sbt coverageReport

The coverage report will be available under target/scale-2.10/scoverage-report directory.

Run application
        
        $ spark-submit ~/Alchemy/core/target/scala-2.10/Alchemy-assembly-0.1.jar --component itr --config ~/Alchemy/core/src/main/resources/config.json
        
#### Options


           --component <value>
                Component name like 'itr/acquisition' etc.
           
           --tablesJson <value>
                Path to data acquisition tables json config file. (optional, used in case of data acquisition)
                
           --config <value>
                Path to Alchemy config file
                


### Application config json schema


           {
               "applicationName": "Name of the application",
               "master": "local or local[4] or local[*] or spark://master:7077 to run on a Spark standalone cluster",
               "basePath": "Base path of the location where the data will be saved",
               "credentials": [
                    {
                             "source": "source of the data",
                             "driver": "driver to be used",
                             "server": "IP where the database is hosted",
                             "port": "The port number",
                             "dbName": "Database name",
                             "userName": "Username to connect to the database",
                             "password": "Password for the above username"
                    }
               ]
           }
