package com.jabong.dap.model.customer

import com.jabong.dap.common.{DataFiles, SharedSparkContext, Spark}
import com.jabong.dap.model.customer.variables.Customer
import org.apache.spark.sql.functions._
import com.jabong.dap.model.schema.Schema
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{FlatSpec}

/**
  * Created by raghu on 16/6/15.
  */
class CustomerTest extends FlatSpec with SharedSparkContext{


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

     dfCustomer = readFromJson(DataFiles.CUSTOMER, DataFiles.CUSTOMER)
     dfNLS = readFromJson(DataFiles.NEWSLETTER_SUBSCRIPTION, DataFiles.NEWSLETTER_SUBSCRIPTION)
     dfSalesOrder = readFromJson(DataFiles.SALES_ORDER, DataFiles.SALES_ORDER)
     dfCSH = readFromJson(DataFiles.CUSTOMER_STORECREDITS_HISTORY, DataFiles.CUSTOMER_STORECREDITS_HISTORY)
     dfCustomerSegments = readFromJson(DataFiles.CUSTOMER_SEGMENTS, DataFiles.CUSTOMER_SEGMENTS)

   }

   def writeToJson(fileName: String): Any={

             var BOB_PATH =  "/home/raghu/bigData/parquetFiles/"

             val df = Spark.getSqlContext().read.parquet(BOB_PATH + fileName + "/")

             df.limit(10).select("*").write.format("json").json(DataFiles.TEST_RESOURCES + fileName + ".json")

   }

  def readFromJson(directoryName:String, fileName: String): DataFrame = {

            val df = Spark.getSqlContext().read.format("json").load(DataFiles.TEST_RESOURCES + directoryName + "/" + fileName + ".json")

            return df
  }


  "AccRegDateAndUpdatedAt Data Frame" should "match to resultant Data Frame" in {

           //Name of variable: EMAIL, ACC_REG_DATE, UPDATED_AT
           val result = Customer.getAccRegDateAndUpdatedAt(dfCustomer: DataFrame,
                                                           dfNLS: DataFrame,
                                                           dfSalesOrder: DataFrame).limit(30).collect().toSet

//           result.limit(30).write.json(DataFiles.TEST_RESOURCES + "accRegDate_updatedAt" + ".json")

           val dfAccRegDateAndUpdatedAt = readFromJson(DataFiles.CUSTOMER, "accRegDate_updatedAt")
                                                        .select("email", "acc_reg_date", "updated_at")
                                                        .collect().toSet

           assert(result.equals(dfAccRegDateAndUpdatedAt) == true)

   }

  "Data Frame" should "null" in {

            //Name of variable: EMAIL, ACC_REG_DATE, UPDATED_AT
            val result = Customer.getAccRegDateAndUpdatedAt(null, null, null)

            assert(result == null)

  }

  "schema attributes" should "present in data frames" in {

            val BOB_PATH =  "/home/raghu/bigData/parquetFiles/"

            val dfCustomer = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.CUSTOMER + "/")
            val dfNLS = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.NEWSLETTER_SUBSCRIPTION + "/")
            val dfSalesOrder = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.SALES_ORDER + "/")
            val dfCSH = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.CUSTOMER_STORECREDITS_HISTORY + "/")
            val dfCustomerSegments = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.CUSTOMER_SEGMENTS + "/")

            var result = true

            if(dfCustomer == null ||
               dfNLS == null ||
               dfSalesOrder == null ||
               dfCSH == null ||
               dfCustomerSegments == null){

              log("Data frame should not be null")

              result = false
            }
            else if(!Schema.isCustomerSchema(dfCustomer) ||
                    !Schema.isNLSSchema(dfNLS) ||
                    !Schema.isSalesOrderSchema(dfSalesOrder) ||
                    !Schema.isCSHSchema(dfCSH) ||
                    !Schema.isCustomerSegmentsSchema(dfCustomerSegments)){

                  log("schema attributes or data type mismatch")

                  result = false

            }

            assert(result == true)

  }



 }
