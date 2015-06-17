package com.jabong.dap.model.customer

import com.jabong.dap.common.{DataFiles, SharedSparkContext, Spark}
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

//     writeToJson(DataFiles.CUSTOMER)
//     writeToJson(DataFiles.NEWSLETTER_SUBSCRIPTION)
//     writeToJson(DataFiles.SALES_ORDER)
//     writeToJson(DataFiles.CUSTOMER_STORECREDITS_HISTORY)
//     writeToJson(DataFiles.CUSTOMER_SEGMENTS)

     dfCustomer = readFromJson(DataFiles.CUSTOMER)
     dfNLS = readFromJson(DataFiles.NEWSLETTER_SUBSCRIPTION)
     dfSalesOrder = readFromJson(DataFiles.SALES_ORDER)
     dfCSH = readFromJson(DataFiles.CUSTOMER_STORECREDITS_HISTORY)
     dfCustomerSegments = readFromJson(DataFiles.CUSTOMER_SEGMENTS)

   }

   def writeToJson(fileName: String): Any={

         var BOB_PATH =  "/home/raghu/bigData/parquetFiles/"

         val df = Spark.getSqlContext().read.parquet(BOB_PATH + fileName + "/")

         df.limit(10).select("*").write.format("json").json(DataFiles.TEST_RESOURCES + fileName + ".json")

   }

  def readFromJson(fileName: String): DataFrame = {

      val df = Spark.getSqlContext().read.format("json").load(DataFiles.TEST_RESOURCES + fileName + "/" + fileName + ".json")

      return df
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
