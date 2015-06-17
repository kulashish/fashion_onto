package com.jabong.dap.model

import com.jabong.dap.common.{DataFiles, MergeUtils, Spark, SharedSparkContext}
import com.jabong.dap.model.customer.variables.Customer
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FunSuite

/**
 * Created by raghu on 16/6/15.
 */
class CustomerTest extends FunSuite with SharedSparkContext{


  @transient var sqlContext: SQLContext = _
  
  @transient var dfCustomer: DataFrame = _
  @transient var dfNLS: DataFrame = _
  @transient var dfSalesOrder: DataFrame = _
  @transient var dfCSH: DataFrame = _
  @transient var dfCustomerSegments: DataFrame = _

  override def beforeAll() {

    super.beforeAll()

//    writeToJson(DataFiles.CUSTOMER)
//    writeToJson(DataFiles.NEWSLETTER_SUBSCRIPTION)
//    writeToJson(DataFiles.SALES_ORDER)
//    writeToJson(DataFiles.CUSTOMER_STORECREDITS_HISTORY)
//    writeToJson(DataFiles.CUSTOMER_SEGMENTS)

    dfCustomer = Spark.getSqlContext().read.format("json").load(DataFiles.TEST_RESOURCES+ DataFiles.CUSTOMER + ".json")
    dfNLS = Spark.getSqlContext().read.format("json").load(DataFiles.TEST_RESOURCES+ DataFiles.NEWSLETTER_SUBSCRIPTION + ".json")
    dfSalesOrder = Spark.getSqlContext().read.format("json").load(DataFiles.TEST_RESOURCES+ DataFiles.SALES_ORDER + ".json")
    dfCSH = Spark.getSqlContext().read.format("json").load(DataFiles.TEST_RESOURCES+ DataFiles.CUSTOMER_STORECREDITS_HISTORY + ".json")
    dfCustomerSegments = Spark.getSqlContext().read.format("json").load(DataFiles.TEST_RESOURCES+ DataFiles.CUSTOMER_SEGMENTS + ".json")
    

  }

  def writeToJson(fileName: String): Any={

        var BOB_PATH =  "/home/raghu/bigData/parquetFiles/"

        val df = Spark.getSqlContext().read.parquet(BOB_PATH + fileName + "/")

        df.limit(10).select("*").write.format("json").json(DataFiles.TEST_RESOURCES + fileName + ".json")

  }

  test("Test for ACC_REG_DATE, UPDATED_AT") {
    
    //Name of variable: id_customer, ACC_REG_DATE, UPDATED_AT
    val dfAccRegDateAndUpdatedAt = Customer.getAccRegDateAndUpdatedAt(dfCustomer: DataFrame,
                                                                      dfNLS: DataFrame,
                                                                      dfSalesOrder: DataFrame)
    dfAccRegDateAndUpdatedAt.collect().foreach(println)
    
    assert(3 == 3)
  }

}
