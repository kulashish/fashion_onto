package com.jabong.dap.init

import com.jabong.dap.data.acq.Delegator
import scopt.OptionParser
import com.jabong.dap.model.product.itr.Itr

/**
 * Created by Apoorva Moghey on 04/06/15.
 */

object Init {

  case class Params(component: String = null, master: String = null)

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
      arg[String]("<component>")
        .text("Component name like 'itr' etc.")
        .required()
        .action((x, c) => c.copy(component = x))
        .validate(x =>
          if (x == "itr" || x == "data-acquisition") success else failure("Option <component> must contain valid value. Like itr"))

      arg[String]("<master>")
        .text("Master name like 'local' etc.")
        .required()
        .action((x, c) => c.copy(master = x))
        .validate((x) => if (x == "local" || x == "local[4]") success else failure("Option <master> must contain valid value. Like local"))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    params.component match {
      case "itr" => new Itr(params.master).start()
      case "data-acquisition" => new Delegator(params.master).start()
    }
  }
}
