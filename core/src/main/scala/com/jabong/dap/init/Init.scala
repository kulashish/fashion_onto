package com.jabong.dap.init

import scopt.OptionParser
import java.nio.file.{Paths, Files}

object Init {

  case class Params(component: String = null, tableJson: String = null, config: String = null)

  def main(args: Array[String]) {
    options(args)
  }

  /**
   * Check for command line options
   * kick action based upon action
   * action passed.
   * @param args Array[String]
   */
  def options(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Alchemy") {
      opt[String]("component")
        .text("Component name like 'itr/acquisition' etc.")
        .required()
        .action((x, c) => c.copy(component = x))
        .validate(x =>
        if (x == "itr") success else failure("Option --component must contain valid value. Like itr."))

      opt[String]("tablesJson")
       .text("Path to data acquisition tables json config file.")
       .action((x, c) => c.copy(tableJson = x))
       .validate(x => if (Files.exists(Paths.get(x))) success else failure("Option --tablesJson path to data acquisition tables list json."))

      opt[String]("config")
       .text("Path to Alchemy config file.")
       .action((x, c) => c.copy(config = x))
       .validate(x => if (Files.exists(Paths.get(x))) success else failure("Option --config path to Alchemy config json."))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    params.component match {
      case "itr" => // do your stuff here
      case "acquisition" => // do your stuff here
    }
  }
}