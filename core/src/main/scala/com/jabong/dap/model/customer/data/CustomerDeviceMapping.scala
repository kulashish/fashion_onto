package com.jabong.dap.model.customer.data

import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.Spark
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.{ SparkConf}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Created by mubarak on 15/7/15.
 */
object CustomerDeviceMapping {

  def getLatestDevice(clickStreamInc: DataFrame, dcf:DataFrame, customer:DataFrame):DataFrame={
    //val filData = clickStreamInc.filter(!clickStreamInc(CustomerVariables.USERID).startsWith(CustomerVariables.APP_FILTER))
    val clickStream = clickStreamInc.orderBy(CustomerVariables.PAGETS).groupBy(CustomerVariables.USERID).agg(
      first(CustomerVariables.BROWSER_ID) as CustomerVariables.BROWSER_ID,
      first(CustomerVariables.DOMAIN) as CustomerVariables.DOMAIN)

    // outerjoin with customer table one day increment on userid = email
    // id_customer, email, browser_id, domain
    val broCust = Spark.getContext().broadcast(customer).value
    val joinedDf = clickStream.join(broCust,broCust(CustomerVariables.EMAIL) === clickStream(CustomerVariables.USERID),"outer")
      .select(coalesce(broCust(CustomerVariables.EMAIL),clickStream(CustomerVariables.USERID)) as CustomerVariables.EMAIL,
      broCust(CustomerVariables.ID_CUSTOMER),
      clickStream(CustomerVariables.BROWSER_ID),
      clickStream(CustomerVariables.DOMAIN)
    )
    val joined = joinedDf.join(dcf, dcf(CustomerVariables.EMAIL) === joinedDf(CustomerVariables.EMAIL), "outer").select(
      coalesce(dcf(CustomerVariables.EMAIL),joinedDf(CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,
      dcf(CustomerVariables.RESPONSYS_ID),
      dcf(CustomerVariables.ID_CUSTOMER),
      coalesce(dcf(CustomerVariables.BROWSER_ID),joinedDf(CustomerVariables.BROWSER_ID)) as CustomerVariables.BROWSER_ID,
      coalesce(dcf(CustomerVariables.DOMAIN),joinedDf(CustomerVariables.DOMAIN)) as CustomerVariables.DOMAIN)
    joined
  }

   def processData(prevDate:String, path:String, curDate: String) {
     val df1 = DataReader.getDataFrame(DataSets.OUTPUT_PATH,DataSets.CLICKSTREAM,DataSets.USER_DEVICE_MAP_APP, DataSets.DAILY_MODE, curDate)
     df1.printSchema()
     df1.show(5)
     var df2: DataFrame = null
     if (null != path) {
       df2 = DataReader.getDataFrameCsv4mDCF(path,";")
     } else {
       df2 = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.DAILY_MODE, prevDate)
     }
     df2.printSchema()
     df2.show(10)
     val df3 = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.DAILY_MODE, curDate)
     val res = getLatestDevice(df1,df2, df3)
     res.printSchema()
     res.show(20)
     DataWriter.writeParquet(res, DataSets.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.DAILY_MODE, curDate)
   }

}
