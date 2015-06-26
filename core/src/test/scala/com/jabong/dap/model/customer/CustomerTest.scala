package com.jabong.dap.model.customer

import com.jabong.dap.common.{DataFiles, SharedSparkContext, Spark}
import com.jabong.dap.model.customer.variables.{CustomerSegments, CustomerStorecreditsHistory, Customer}
import org.apache.spark.sql.functions._
import com.jabong.dap.model.schema.Schema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
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

     dfCustomer = readFromJson(DataFiles.CUSTOMER, DataFiles.CUSTOMER, Schema.customer)
     dfNLS = readFromJson(DataFiles.NEWSLETTER_SUBSCRIPTION, DataFiles.NEWSLETTER_SUBSCRIPTION, Schema.nls)
     dfSalesOrder = readFromJson(DataFiles.SALES_ORDER, DataFiles.SALES_ORDER, Schema.salesOrder)
     dfCSH = readFromJson(DataFiles.CUSTOMER_STORECREDITS_HISTORY, DataFiles.CUSTOMER_STORECREDITS_HISTORY, Schema.csh)
     dfCustomerSegments = readFromJson(DataFiles.CUSTOMER_SEGMENTS, DataFiles.CUSTOMER_SEGMENTS, Schema.customerSegments)

   }

   def writeToJson(fileName: String): Any={

       var BOB_PATH =  "/home/raghu/bigData/parquetFiles/"

       val df = Spark.getSqlContext().read.parquet(BOB_PATH + fileName + "/")

       df.limit(10).select("*").write.format("json").json(DataFiles.TEST_RESOURCES + fileName + ".json")

   }

  def readFromJson(directoryName:String, fileName: String, schema: StructType): DataFrame = {

      val df = Spark.getSqlContext().read.schema(schema).format("json")
                    .load(DataFiles.TEST_RESOURCES + directoryName + "/" + fileName + ".json")

      df
  }

//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  // //schema attributes or data type should be match
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  "schema attributes and type" should "match into DataFrames" in {
//
//        val BOB_PATH =  "/home/raghu/bigData/parquetFiles/"
//
//        val dfCustomer = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.CUSTOMER + "/")
//        val dfNLS = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.NEWSLETTER_SUBSCRIPTION + "/")
//        val dfSalesOrder = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.SALES_ORDER + "/")
//        val dfCSH = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.CUSTOMER_STORECREDITS_HISTORY + "/")
//        val dfCustomerSegments = Spark.getSqlContext().read.parquet(BOB_PATH + DataFiles.CUSTOMER_SEGMENTS + "/")
//
//        var result = true
//
//        if(dfCustomer == null ||
//          dfNLS == null ||
//          dfSalesOrder == null ||
//          dfCSH == null ||
//          dfCustomerSegments == null){
//
//          log("Data frame should not be null")
//
//          result = false
//        }
//        else if(!Schema.isEquals(dfCustomer.schema, Schema.customer) ||
//                !Schema.isEquals(dfNLS.schema, Schema.nls) ||
//                !Schema.isEquals(dfSalesOrder.schema, Schema.salesOrder) ||
//                !Schema.isEquals(dfCSH.schema, Schema.csh) ||
//                !Schema.isEquals(dfCustomerSegments.schema, Schema.customerSegments)){
//
//          log("schema attributes or data type mismatch")
//
//          result = false
//
//        }
//
//        assert(result == true)
//
//  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // //Name of variable: EMAIL, ACC_REG_DATE, UPDATED_AT
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getCustomer: Data Frame dfCustomer, dfNLS, dfSalesOrder" should "null" in {

    val result = Customer.getCustomer(null, null, null)

    assert(result == null)

  }

  "getCustomer: schema attributes and data type" should
    "match into DataFrames(dfCustomer, dfNLS, dfSalesOrder)" in {

    val result = Customer.getCustomer(dfCustomer: DataFrame,
                                      dfNLS: DataFrame,
                                      dfSalesOrder: DataFrame)
    assert(result != null)

  }

  "getCustomer: Data Frame" should "match to resultant Data Frame" in {

    val result = Customer.getCustomer(dfCustomer: DataFrame,
                                                    dfNLS: DataFrame,
                                                    dfSalesOrder: DataFrame)
//                          .limit(30).collect().toSet

//           result.limit(30).write.json(DataFiles.TEST_RESOURCES + "result_customer" + ".json")

//    val dfAccRegDateAndUpdatedAt = readFromJson(DataFiles.CUSTOMER, "accRegDate_updatedAt",
//                                                Customer.accRegDateAndUpdatedAt)
//                                                .collect().toSet
//
//    assert(result.equals(dfAccRegDateAndUpdatedAt) == true)

  }
//
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  // //Name of variable: EMAIL, ACC_REG_DATE, UPDATED_AT
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  "getAccRegDateAndUpdatedAt: Data Frame" should "match to resultant Data Frame" in {
//
//       val result = Customer.getAccRegDateAndUpdatedAt(dfCustomer: DataFrame,
//                                                       dfNLS: DataFrame,
//                                                       dfSalesOrder: DataFrame).limit(30).collect().toSet
//
////       result.limit(30).write.json(DataFiles.TEST_RESOURCES + "accRegDate_updatedAt" + ".json")
//
//       val dfAccRegDateAndUpdatedAt = readFromJson(DataFiles.CUSTOMER, "accRegDate_updatedAt",
//                                                   Customer.accRegDateAndUpdatedAt)
//                                                  .collect().toSet
//
//       assert(result.equals(dfAccRegDateAndUpdatedAt) == true)
//
//   }
//
//
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  //Name of variable: id_customer, EMAIL_OPT_IN_STATUS
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  "getEmailOptInStatus: Data Frame" should "match to resultant Data Frame" in {
//
//        val result = Customer.getEmailOptInStatus(dfCustomer: DataFrame, dfNLS: DataFrame)
//                             .limit(10).collect().toSet
//
//    //    result.limit(10).write.json(DataFiles.TEST_RESOURCES + "emailOptInStatus" + ".json")
//
//        val dfEmailOptInStatus = readFromJson(DataFiles.CUSTOMER, "email_opt_in_status",  Customer.email_opt_in_status)
//                                             .collect().toSet
//
//        assert(result.equals(dfEmailOptInStatus) == true)
//
//  }
//
//  "getEmailOptInStatus: getStatusValue " should "o" in {
//
//        val row = Row("", null)
//
//        val result = Customer.getStatusValue(row)
//
//        assert(result == "o")
//
//  }
//
//  "getEmailOptInStatus: getStatusValue " should "iou" in {
//
//        val row = Row("", "subscribed")
//
//        val result = Customer.getStatusValue(row)
//
//        assert(result == "iou")
//
//  }
//
//  "getEmailOptInStatus: getStatusValue " should "u" in {
//
//        val row = Row("", "unsubscribed")
//
//        val result = Customer.getStatusValue(row)
//
//        assert(result == "u")
//
//  }
//
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  //Name of variable: id_customer, CUSTOMERS PREFERRED ORDER TIMESLOT
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  "getCustomersPreferredOrderTimeslot: Data Frame" should "match to resultant Data Frame" in {
//
//        val result = Customer.getCustomersPreferredOrderTimeslot(dfSalesOrder: DataFrame)
//                             .limit(30).collect().toSet
//
////        result.limit(30).write.json(DataFiles.TEST_RESOURCES + "customers_preferred_order_timeslot" + ".json")
//
//        val dfCustomersPreferredOrderTimeslot = readFromJson(DataFiles.CUSTOMER, "customers_preferred_order_timeslot",
//                                                             Customer.customers_preferred_order_timeslot)
//                                                            .collect().toSet
//
//        assert(result.equals(dfCustomersPreferredOrderTimeslot) == true)
//
//  }
//
//
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  //Name of variable: fk_customer, LAST_JR_COVERT_DATE
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  "getLastJrCovertDate: Data Frame dfCSH" should "null" in {
//
//        val result = CustomerStorecreditsHistory.getLastJrCovertDate(null)
//
//        assert(result == null)
//
//  }
//
//  "getLastJrCovertDate: schema attributes and data type" should
//    "match into DataFrame(dfCSH)" in {
//
//        val result = CustomerStorecreditsHistory.getLastJrCovertDate(dfCSH: DataFrame)
//
//        assert(result != null)
//
//  }
//
//  "getLastJrCovertDate: Data Frame" should "match to resultant Data Frame" in {
//
//        val result = CustomerStorecreditsHistory.getLastJrCovertDate(dfCSH: DataFrame)
//                             .limit(30).collect().toSet
//
////                result.limit(30).write.json(DataFiles.TEST_RESOURCES + "last_jr_covert_date" + ".json")
//
//        val dfLastJrCovertDate = readFromJson(DataFiles.CUSTOMER, "last_jr_covert_date",
//                                              CustomerStorecreditsHistory.last_jr_covert_date)
//                                              .collect().toSet
//
//        assert(result.equals(dfLastJrCovertDate) == true)
//
//  }
//
//
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  //Name of variable: fk_customer, MVP, Segment0, Segment1,Segment2, Segment3, Segment4, Segment5, Segment6
//  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  "getMvpAndSeg: Data Frame dfCustomerSegments" should "null" in {
//
//        val result = CustomerSegments.getMvpAndSeg(null)
//
//        assert(result == null)
//
//  }
//
//  "getMvpAndSeg: schema attributes and data type" should
//    "match into DataFrame(dfCSH)" in {
//
//        val result = CustomerSegments.getMvpAndSeg(dfCustomerSegments: DataFrame)
//
//        assert(result != null)
//
//  }
//
//  "getMvpAndSeg: Data Frame" should "match to resultant Data Frame" in {
//
//        val result = CustomerSegments.getMvpAndSeg(dfCustomerSegments: DataFrame)
//                             .limit(30).collect().toSet
//
//        //                result.limit(30).write.json(DataFiles.TEST_RESOURCES + "mvp_seg" + ".json")
//
//        val dfMvpSeg = readFromJson(DataFiles.CUSTOMER, "mvp_seg", CustomerSegments.mvp_seg)
//                                    .collect().toSet
//
//        assert(result.equals(dfMvpSeg) == true)
//
//  }

 }
