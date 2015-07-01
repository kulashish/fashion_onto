package com.jabong.dap.model.customer

import java.sql.Timestamp

import com.jabong.dap.common.{Utils, DataFiles, SharedSparkContext, Spark}
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

  //     Utils.writeToJson(DataFiles.CUSTOMER)
  //     Utils.writeToJson(DataFiles.NEWSLETTER_SUBSCRIPTION)
  //     Utils.writeToJson(DataFiles.SALES_ORDER)
  //     Utils.writeToJson(DataFiles.CUSTOMER_STORECREDITS_HISTORY)
  //     Utils.writeToJson(DataFiles.CUSTOMER_SEGMENTS)

     dfCustomer = Utils.readFromJson(DataFiles.CUSTOMER, DataFiles.CUSTOMER, Schema.customer)
     dfNLS = Utils.readFromJson(DataFiles.NEWSLETTER_SUBSCRIPTION, DataFiles.NEWSLETTER_SUBSCRIPTION, Schema.nls)
     dfSalesOrder = Utils.readFromJson(DataFiles.SALES_ORDER, DataFiles.SALES_ORDER, Schema.salesOrder)
     dfCSH = Utils.readFromJson(DataFiles.CUSTOMER_STORECREDITS_HISTORY, DataFiles.CUSTOMER_STORECREDITS_HISTORY, Schema.csh)
     dfCustomerSegments = Utils.readFromJson(DataFiles.CUSTOMER_SEGMENTS, DataFiles.CUSTOMER_SEGMENTS, Schema.customerSegments)

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
                              .limit(30).collect().toSet

//               result.limit(30).write.json(DataFiles.TEST_RESOURCES + "result_customer" + ".json")

        val dfResultCustomer = Utils.readFromJson(DataFiles.CUSTOMER, "result_customer",
                                                    Customer.result_customer)
                                                    .collect().toSet

        assert(result.equals(dfResultCustomer) == true)

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Name of variable: ACC_REG_DATE
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getMin(): timestamp t1 and t2 value " should "be null" in {

       val t1 = null

       val t2 = null

       val result = Customer.getMin(t1, t2)

       assert(result == null)

   }

  "getMin(): timestamp t1" should "be null" in {

        val t1 = null

        val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

        val result = Customer.getMin(t1, t2)

        assert(result.compareTo(t2) == 0)

  }

  "getMin(): timestamp t2" should "be null" in {

        val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

        val t2 = null

        val result = Customer.getMin(t1, t2)

        assert(result.compareTo(t1) == 0)

  }

  "getMin(): return timestamp " should "t1" in {

        val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

        val t2 = Timestamp.valueOf("2015-04-30 00:05:09.0")

        val result = Customer.getMin(t1, t2)

        assert(result.compareTo(t1) >= 0)

  }

  "getMin(): return timestamp " should "t2" in {

        val t1 = Timestamp.valueOf("2015-04-30 00:05:09.0")

        val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

        val result = Customer.getMin(t1, t2)

        assert(result.compareTo(t2) >= 0)

  }



  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Name of variable: MAX_UPDATED_AT
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getMax(): timestamp t1 and t2 value " should "be null" in {

        val t1 = null

        val t2 = null

        val result = Customer.getMax(t1, t2)

        assert(result == null)

  }

  "getMax(): timestamp t1" should "be null" in {

        val t1 = null

        val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

        val result = Customer.getMax(t1, t2)

        assert(result.compareTo(t2) == 0)

  }

  "getMax(): timestamp t2" should "be null" in {

        val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

        val t2 = null

        val result = Customer.getMax(t1, t2)

        assert(result.compareTo(t1) == 0)

  }

  "getMax(): return timestamp " should "t2" in {

        val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

        val t2 = Timestamp.valueOf("2015-04-30 00:05:09.0")

        val result = Customer.getMax(t1, t2)

        assert(result.compareTo(t2) == 0)

  }

  "getMax(): return timestamp " should "t1" in {

        val t1 = Timestamp.valueOf("2015-04-30 00:05:09.0")

        val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

        val result = Customer.getMax(t1, t2)

        assert(result.compareTo(t1) == 0)

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: EMAIL_OPT_IN_STATUS
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getEmailOptInStatus: getStatusValue " should "o" in {

        val result = Customer.getEmailOptInStatus(null, null)

        assert(result == "o")

  }

  "getEmailOptInStatus: getStatusValue " should "iou" in {

        val row = Row("", "subscribed")

        val result = Customer.getEmailOptInStatus("1", "subscribed")

        assert(result == "iou")

  }

  "getEmailOptInStatus: getStatusValue " should "u" in {

        val row = Row("", "unsubscribed")

        val result = Customer.getEmailOptInStatus("1", "unsubscribed")

        assert(result == "u")

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: id_customer, CUSTOMERS PREFERRED ORDER TIMESLOT
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getCustomersPreferredOrderTimeslot: Data Frame" should "match to resultant Data Frame" in {

        val result = Customer.getCPOT(dfSalesOrder: DataFrame)
                             .limit(30).collect().toSet

//        result.limit(30).write.json(DataFiles.TEST_RESOURCES + "customers_preferred_order_timeslot" + ".json")

        val dfCustomersPreferredOrderTimeslot = Utils.readFromJson(DataFiles.CUSTOMER, "customers_preferred_order_timeslot",
                                                             Customer.customers_preferred_order_timeslot)
                                                            .collect().toSet

        assert(result.equals(dfCustomersPreferredOrderTimeslot) == true)

  }








  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: fk_customer, LAST_JR_COVERT_DATE
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getLastJrCovertDate: Data Frame dfCSH" should "null" in {

        val result = CustomerStorecreditsHistory.getLastJrCovertDate(null)

        assert(result == null)

  }

  "getLastJrCovertDate: schema attributes and data type" should
    "match into DataFrame(dfCSH)" in {

        val result = CustomerStorecreditsHistory.getLastJrCovertDate(dfCSH: DataFrame)

        assert(result != null)

  }

  "getLastJrCovertDate: Data Frame" should "match to resultant Data Frame" in {

        val result = CustomerStorecreditsHistory.getLastJrCovertDate(dfCSH: DataFrame)
                             .limit(30).collect().toSet

//                result.limit(30).write.json(DataFiles.TEST_RESOURCES + "last_jr_covert_date" + ".json")

        val dfLastJrCovertDate = Utils.readFromJson(DataFiles.CUSTOMER, "last_jr_covert_date",
                                              CustomerStorecreditsHistory.last_jr_covert_date)
                                              .collect().toSet

        assert(result.equals(dfLastJrCovertDate) == true)

  }





  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: fk_customer, MVP, Segment0, Segment1,Segment2, Segment3, Segment4, Segment5, Segment6
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getMvpAndSeg: Data Frame dfCustomerSegments" should "null" in {

        val result = CustomerSegments.getMvpAndSeg(null)

        assert(result == null)

  }

  "getMvpAndSeg: schema attributes and data type" should
    "match into DataFrame(dfCSH)" in {

        val result = CustomerSegments.getMvpAndSeg(dfCustomerSegments: DataFrame)

        assert(result != null)

  }

  "getMvpAndSeg: Data Frame" should "match to resultant Data Frame" in {

        val result = CustomerSegments.getMvpAndSeg(dfCustomerSegments: DataFrame)
                             .limit(30).collect().toSet

//                        result.limit(30).write.json(DataFiles.TEST_RESOURCES + "mvp_seg" + ".json")

        val dfMvpSeg = Utils.readFromJson(DataFiles.CUSTOMER, "mvp_seg", CustomerSegments.mvp_seg)
                                    .collect().toSet

        assert(result.equals(dfMvpSeg) == true)

  }

 }
