package com.jabong.dap.model.customer.variables

import java.sql.Timestamp

import com.jabong.dap.common.{Constants, Spark, Utils}
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.utils.Time
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Created by raghu on 27/5/15.
  */
object Customer {

      ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      //customer variable schemas
      ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      val email_opt_in_status = StructType(Array(StructField("id_customer", IntegerType, true),
                                                 StructField("status", StringType, true)))

      val accRegDateAndUpdatedAt = StructType(Array(StructField("email", StringType, true),
                                                    StructField("acc_reg_date", TimestampType, true),
                                                    StructField("updated_at", TimestampType, true)))

      val customers_preferred_order_timeslot = StructType(Array(StructField("fk_customer_cpot", IntegerType, true),
                                                                StructField("customer_all_order_timeslot", StringType, true),
                                                                StructField("customer_preferred_order_timeslot", IntegerType, true)))

      val result_customer =  StructType(Array(StructField("id_customer", IntegerType, true),
                             StructField("giftcard_credits_available", DecimalType(10,2), true),
                             StructField("store_credits_available", DecimalType(10,2), true),
                             StructField("birthday", DateType, true),
                             StructField("gender", StringType, true),
                             StructField("reward_type", StringType, true),
                             StructField("email", StringType, true),
                             StructField("created_at", TimestampType, true),
                             StructField("updated_at", TimestampType, true),
                             StructField("customer_all_order_timeslot", StringType, true),
                             StructField("customer_preferred_order_timeslot", IntegerType, true),
                             StructField("acc_reg_date", TimestampType, true),
                             StructField("max_updated_at", TimestampType, true),
                             StructField("email_opt_in_status", StringType, true)))
    
      ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      // DataFrame Customer,NLS, SalesOrder operations
      ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


      /*        Name of variables:id_customer,
                                  GIFTCARD_CREDITS_AVAILABLE,
                                  STORE_CREDITS_AVAILABLE,
                                  EMAIL,
                                  BIRTHDAY,
                                  GENDER,
                                  PLATINUM_STATUS,
                                  ACC_REG_DATE,
                                  UPDATED_AT,
                                  EMAIL_OPT_IN_STATUS,
                                  CUSTOMERS PREFERRED ORDER TIMESLOT*/
      def getCustomer(dfCustomer: DataFrame, dfNLS: DataFrame, dfSalesOrder: DataFrame): DataFrame = {

          if(dfCustomer == null || dfNLS == null || dfSalesOrder == null ){

            log("Data frame should not be null")

            return null

          }

          if(!Utils.isSchemaEqual(dfCustomer.schema, Schema.customer) ||
             !Utils.isSchemaEqual(dfNLS.schema, Schema.nls) ||
             !Utils.isSchemaEqual(dfSalesOrder.schema, Schema.salesOrder)){

             log("schema attributes or data type mismatch")

            return null

          }


          val NLS = dfNLS.select(col("email") as "nls_email",
                                 col("status"),
                                 col("created_at") as "nls_created_at",
                                 col("updated_at") as "nls_updated_at")

          //Name of variable: CUSTOMERS PREFERRED ORDER TIMESLOT
          val udfCPOT = getCPOT(dfSalesOrder: DataFrame)

          val dfJoin = dfCustomer.select("id_customer",
                                         "giftcard_credits_available",
                                         "store_credits_available",
                                         "birthday",
                                         "gender",
                                         "reward_type",
                                         "email",
                                         "created_at",
                                         "updated_at")
                                        .join(NLS, dfCustomer("email") === NLS("nls_email"), "outer")
                                        .join(dfSalesOrder.select(col("fk_customer"),
                                                                  col("created_at") as "so_created_at",
                                                                  col("updated_at") as "so_updated_at"),
                                         dfCustomer("id_customer") === dfSalesOrder("fk_customer"), "outer")
                                        .join(udfCPOT,
                                        dfCustomer("id_customer") === udfCPOT("fk_customer_cpot"), "outer")


          // Define User Defined Functions
          val sqlContext = Spark.getSqlContext()

          //min(customer.created_at, sales_order.created_at)
          val udfAccRegDate = udf((cust_created_at: Timestamp, nls_created_at: Timestamp)
                               =>getMin(cust_created_at: Timestamp, nls_created_at: Timestamp))

          //max(customer.updated_at, newsletter_subscription.updated_at, sales_order.updated_at)
          val udfMaxUpdatedAt = udf((cust_updated_at: Timestamp, nls_updated_at: Timestamp, so_updated_at: Timestamp)
                                =>getMax(getMax(cust_updated_at: Timestamp, nls_updated_at: Timestamp), so_updated_at: Timestamp))

          //Name of variable: EMAIL_OPT_IN_STATUS
          val udfEmailOptInStatus = udf((nls_email: String, status: String)
                                    => getEmailOptInStatus(nls_email: String, status: String))



          /*        Name of variables:ID_CUSTOMER,
                                     GIFTCARD_CREDITS_AVAILABLE,
                                     STORE_CREDITS_AVAILABLE,
                                     EMAIL,
                                     BIRTHDAY,
                                     GENDER,
                                     PLATINUM_STATUS,
                                     EMAIL,
                                     CREATED_AT,
                                     UPDATED_AT,
                                     CUSTOMER_ALL_ORDER_TIMESLOT,
                                     CUSTOMERS PREFERRED ORDER TIMESLOT,
                                     ACC_REG_DATE,
                                     MAX_UPDATED_AT,
                                     EMAIL_OPT_IN_STATUS,*/
          val dfResult = dfJoin.select(col("id_customer"),
                                       col("giftcard_credits_available"),
                                       col("store_credits_available"),
                                       col("birthday"),
                                       col("gender"),
                                       col("reward_type"),
                                       col("email"),
                                       col("created_at"),
                                       col("updated_at"),
                                       col("customer_all_order_timeslot"),
                                       col("customer_preferred_order_timeslot"),

                                       udfAccRegDate(dfJoin("created_at"),
                                                     dfJoin("nls_created_at")) as "acc_reg_date",

                                       udfMaxUpdatedAt(dfJoin("updated_at"),
                                                       dfJoin("nls_created_at"),
                                                       dfJoin("so_updated_at")) as "max_updated_at",

                                       udfEmailOptInStatus(dfJoin("nls_email"),
                                                           dfJoin("status")) as "email_opt_in_status")

//
//          if (isOldDate) {
//
//            val dfCustomerFull = Spark.getSqlContext().read.parquet(DataFiles.VARIABLE_PATH + DataFiles.CUSTOMER + "/full" + oldDateFolder)
//
//            val custBCVar = Spark.getContext().broadcast(dfResult)
//
//            dfResult = MergeDataImpl.InsertUpdateMerge(dfCustomerFull, custBCVar.value, "id_customer")
//          }

         dfResult
      }

      //min(customer.created_at, sales_order.created_at)
      def getMin(t1: Timestamp, t2: Timestamp): Timestamp ={

          if(t1 == null) {
            return t2
          }

          if(t2 == null) {
            return t1
          }

          if (t1.compareTo(t2) >= 0)
            t1
          else
            t2

      }

      //max(customer.updated_at, newsletter_subscription.updated_at, sales_order.updated_at)
      def getMax(t1: Timestamp, t2: Timestamp): Timestamp ={

          if(t1 == null) {
            return t2
          }

          if(t2 == null) {
            return t1
          }

          if (t1.compareTo(t2) < 0)
            t2
          else
            t1

      }

       //iou - i: opt in(subscribed), o: opt out(when registering they have opted out), u: unsubscribed
       def getEmailOptInStatus(nls_email: String, status: String): String = {

           if(nls_email == null){
             return "o"
           }

           status match {
             case "subscribed" => "iou"
             case "unsubscribed" => "u"
           }

       }

       //CustomersPreferredOrderTimeslot: Time slot: 2 hrs each, start from 7 am. total 12 slots (1 to 12)
       def getCPOT(dfSalesOrder: DataFrame): DataFrame = {

           val salesOrder = dfSalesOrder.select("fk_customer", "created_at")
                                        .sort("fk_customer", "created_at")

           val soMapReduce=salesOrder.map(r=> ((r(0), Time.timeToSlot(r(1).toString, Constants.DATETIME_FORMAT)),1)).reduceByKey(_+_)

           val soNewMap = soMapReduce.map{case(key,value)=>(key._1,(key._2.asInstanceOf[Int],value.toInt))}

           val soGrouped = soNewMap.groupByKey()

           val finalData =  soGrouped.map{case(key,value)=> (key.toString, getCompleteSlotData(value))}

           val rowRDD = finalData.map({case(key,value) => Row(key.toInt, value._1, value._2)})

           // Apply the schema to the RDD.
           val df = Spark.getSqlContext().createDataFrame(rowRDD, customers_preferred_order_timeslot)

           df
       }


       def getCompleteSlotData(iterable: Iterable[(Int,Int)]): Tuple2[String,Int] = {

           var timeSlotArray = new Array[Int](13)

           var maxSlot: Int = -1

           var max: Int = -1

           iterable.foreach { case (slot, value) => if (value > max){maxSlot = slot ;max = value};
                                                                       timeSlotArray(slot) = value }
           new Tuple2(arrayToString(timeSlotArray), maxSlot)
       }

       def arrayToString(array: Array[Int]): String = {

           var arrayConverted:String = ""

           for(i <- 1 to array.length-1) {

             if(i == 1) {
               arrayConverted=array(i).toString
             } else {
               arrayConverted= arrayConverted+"!"+array(i).toString
             }
           }
           arrayConverted
       }
 }
