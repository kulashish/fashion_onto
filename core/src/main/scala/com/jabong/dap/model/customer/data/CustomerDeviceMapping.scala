package com.jabong.dap.model.customer.data

import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.Spark
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
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
    val joinedDf = clickStream.join(broCust,broCust(CustomerVariables.EMAIL) === clickStream(CustomerVariables.USERID),"outer").select(
      coalesce(broCust(CustomerVariables.EMAIL),clickStream(CustomerVariables.USERID)) as CustomerVariables.EMAIL,
      broCust(CustomerVariables.ID_CUSTOMER),
      clickStream(CustomerVariables.BROWSER_ID),
      clickStream(CustomerVariables.DOMAIN)
    )

    val joined = joinedDf.join(dcf, dcf(CustomerVariables.EMAIL) === joinedDf(CustomerVariables.EMAIL), "outer").select(
      coalesce(dcf(CustomerVariables.EMAIL),joinedDf(CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,
      dcf(CustomerVariables.RESPONSYS_ID),
      dcf(CustomerVariables.ID_CUSTOMER),
      when(joinedDf(CustomerVariables.BROWSER_ID)===null,dcf(CustomerVariables.BROWSER_ID)).otherwise(joinedDf(CustomerVariables.BROWSER_ID)) as CustomerVariables.BROWSER_ID,
      when(joinedDf(CustomerVariables.DOMAIN)===null,dcf(CustomerVariables.DOMAIN)).otherwise(joinedDf(CustomerVariables.DOMAIN)) as CustomerVariables.DOMAIN)
    joined
  }


  def tokenize(x:String):(String,String,String,String,String)={
    val t =x.split(";")
    val a = scala.collection.mutable.ListBuffer[String]()
    t.foreach(x => a.+=:(x))
    if (a.length == 3){
      return (a(2),a(1),a(0),null,null)
    }
    return (a(4),a(3),a(2),a(1),a(0))
  }

   def main(date:String) {
     val conf = new SparkConf().setAppName("SparkExamples")
     Spark.init(conf)
     val sc = Spark.getContext()
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//     val df1 =Spark.getSqlContext().read.parquet("/home/jabong/bobdata/userDeviceMapApp")
     val df1 = DataReader.getDataFrame(DataSets.OUTPUT_PATH,DataSets.CLICKSTREAM,DataSets.USER_DEVICE_MAP_APP, DataSets.DAILY_MODE, date)
     df1.printSchema()
     df1.show(5)
//     val csv = sc.textFile("/home/jabong/bobdata/device_mapping_20150714.csv")
//     val x= csv.map(x =>  tokenize(x))
//     val df2 = Spark.getSqlContext().createDataFrame(x).
//      withColumnRenamed("_1",CustomerVariables.RESPONSYS_ID).
//      withColumnRenamed("_2",CustomerVariables.ID_CUSTOMER).
//      withColumnRenamed("_3",CustomerVariables.EMAIL).
//      withColumnRenamed("_4",CustomerVariables.BROWSER_ID).
//      withColumnRenamed("_5",CustomerVariables.DOMAIN)
     val df2 = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.DAILY_MODE, date)
     val df3 = df2.filter(!df2(CustomerVariables.RESPONSYS_ID).startsWith(CustomerVariables.RESPONSYS_ID))
      df3.printSchema()
      df3.show(10)
//     val df4 =Spark.getSqlContext().read.parquet("/home/jabong/bobdata/customer/07/01")
     val df4 = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.DAILY_MODE, date)
     val res = getLatestDevice(df1,df3, df4)
     res.printSchema()
     res.show(20)
   }
}
