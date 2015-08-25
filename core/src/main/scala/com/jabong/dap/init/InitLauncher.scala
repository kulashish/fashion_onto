package com.jabong.dap.init

import org.apache.spark.launcher.SparkLauncher
import scopt.OptionParser

/**
 * Created by pooja on 24/8/15.
 */
object InitLauncher {

  case class Options(
    master: String = "local[*]",
    sparkDriverMemory: String = "2g",
    sparkExecutorMemory: String = "9g",
    sparkExecutorCores: String = "3",
    sparkDriverExtraClassPath: String = "/usr/share/java/mysql-connector-java-5.1.17.jar",
    verbose: Boolean = true,
    appResource: String = null,
    mainClass: String = "com.jabong.dap.init.Init",
    component: String = null,
    tableJson: String = null,
    mergeJson: String = null,
    paramJson: String = null,
    pushCampaignsJson: String = null,
    config: String = null)

  def GetOptions(args: Array[String]): Unit = {
    val defaultOpts = Options()

    val parser = new OptionParser[Options]("Alchemy") {
      opt[String]("master")
        .text("Master like yarn-cluster/local[*]/yarn-client . Default is local[*]")
        //TODO Add validation for the possible values.
        //.validate(x => )
        .action((x, c) => c.copy(master = x))

      opt[String]("sparkDriverMemory")
        .text("Spark Driver Memory Default is 2g")
        .action((x, c) => c.copy(sparkDriverMemory = x))

      opt[String]("sparkExecutorMemory")
        .text("spark executor memory Default is 9g")
        .action((x, c) => c.copy(sparkExecutorMemory = x))

      opt[String]("sparkExecutorCores")
        .text("spark executor cores Default is 3")
        .action((x, c) => c.copy(sparkExecutorCores = x))

      opt[String]("sparkDriverExtraClassPath")
        .text("spark Driver Extra Classpath. Default is /usr/share/java/mysql-connector-java-5.1.17.jar")
        .action((x, c) => c.copy(sparkDriverExtraClassPath = x))

      opt[Boolean]("verbose")
        .text("verbose is a flag")
        .action ((_, c) => c.copy(verbose = true))

      opt[String]("appResource")
        .text("Jar to be executed e.g., /opt/alchemy-core/current/jar/Alchemy-assembly.jar")
        .required()
        .action((x, c) => c.copy(appResource = x))

      opt[String]("mainClass")
        .text("Class containing the Main: com.jabong.dap.init.Init")
        .required()
        .action((x, c) => c.copy(mainClass = x))

      opt[String]("component")
        .text("Component name like 'itr/acquisition/erp/campaign' etc.")
        .required()
        .action((x, c) => c.copy(component = x))

      opt[String]("config")
        .text("Path to Alchemy config file.")
        .required()
        .action((x, c) => c.copy(config = x))

      opt[String]("mergeJson")
        .text("Path to merge job json config file.")
        .action((x, c) => c.copy(mergeJson = x))

      opt[String]("tablesJson")
        .text("Path to data acquisition tables json config file.")
        .action((x, c) => c.copy(tableJson = x))

      opt[String]("paramJson")
        .text("Path to customer and Order variables merge job json config file.")
        .action((x, c) => c.copy(paramJson = x))

      opt[String]("pushCampaignsJson")
        .text("Path to push Campaigns priority config file.")
        .action((x, c) => c.copy(pushCampaignsJson = x))
    }
    parser.parse(args, defaultOpts).map { opts =>
      run(opts)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def main(args: Array[String]) {
    GetOptions(args)
  }

  def run(opts: Options): Unit = {
    System.out.println("Inside the main")
    var sl: SparkLauncher = new SparkLauncher()
      .setSparkHome("/ext/spark")
      .setMaster(opts.master)
      .setConf(SparkLauncher.DRIVER_MEMORY, opts.sparkDriverMemory)
      .setConf(SparkLauncher.EXECUTOR_MEMORY, opts.sparkExecutorMemory)
      .setConf(SparkLauncher.EXECUTOR_CORES, opts.sparkExecutorCores)
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, opts.sparkDriverExtraClassPath)
      .setVerbose(opts.verbose)
      .setAppResource(opts.appResource)
      .setMainClass(opts.mainClass)
      .addAppArgs("--component")
      .addAppArgs(opts.component)
      .addAppArgs("--config")
      .addAppArgs(opts.config)
    if (null != opts.tableJson) {
      sl = sl.addAppArgs("--tablesJson")
        .addAppArgs(opts.tableJson)
    }
    if (null != opts.mergeJson) {
      sl = sl.addAppArgs("--mergeJson")
        .addAppArgs(opts.mergeJson)
    }
    if (null != opts.paramJson) {
      sl = sl.addAppArgs("--paramJson")
        .addAppArgs(opts.paramJson)
    }
    if (null != opts.pushCampaignsJson) {
      sl = sl.addAppArgs("--pushCampaignsJson")
        .addAppArgs(opts.pushCampaignsJson)
    }
    val spark: Process = sl.launch
    System.out.println("After Launch")
    spark.waitFor
    System.out.println("Done")
  }

}
