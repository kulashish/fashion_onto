package com.jabong.dap.model.customer.variables

import java.sql.Timestamp

import com.jabong.dap.common.{Constants, Utils, Spark}
import com.jabong.dap.model.schema.Schema
import com.jabong.dap.utils.Time
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}


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

      val customers_preferred_order_timeslot = StructType(Array(StructField("fk_customer", IntegerType, true),
                                                                StructField("customer_all_order_timeslot", StringType, true),
                                                                StructField("customer_preferred_order_timeslot", IntegerType, true)))




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

          if(!Utils.isSchemaEquals(dfCustomer.schema, Schema.customer) ||
             !Utils.isSchemaEquals(dfNLS.schema, Schema.nls) ||
             !Utils.isSchemaEquals(dfSalesOrder.schema, Schema.salesOrder)){

             log("schema attributes or data type mismatch")

             return null

          }


        val NLS = dfNLS.withColumnRenamed("email", "nls_email").select("nls_email", "status", "created_at", "updated_at")
          .withColumnRenamed("created_at","nls_created_at").withColumnRenamed("updated_at","nls_updated_at")

         val dfJoin = dfCustomer.select("id_customer",
                                         "giftcard_credits_available",
                                         "store_credits_available",
                                         "birthday",
                                         "gender",
                                         "reward_type",
                                         "email",
                                         "created_at",
                                         "updated_at")
                                        .join(NLS,
                                                         dfCustomer("email") === NLS("nls_email"), "outer")
                                        .join(dfSalesOrder.select(col("fk_customer"),
                                                                 col("created_at") as "so_created_at",
                                                                 col("updated_at") as "so_updated_at"),
                                                                 dfCustomer("id_customer") === dfSalesOrder("fk_customer"), "outer")


        // Define User Defined Functions
        val sqlContext = Spark.getSqlContext()
        import sqlContext.implicits._

        //min(customer.created_at, sales_order.created_at)
        val udfAccRegDate = udf((cust_created_at: Timestamp, nls_created_at: Timestamp)
                             =>min(cust_created_at: Timestamp, nls_created_at: Timestamp))

        //max(customer.updated_at, newsletter_subscription.updated_at, sales_order.updated_at)
        val udfMaxUpdatedAt = udf((cust_updated_at: Timestamp, nls_updated_at: Timestamp, so_updated_at: Timestamp)
                              =>max(max(cust_updated_at: Timestamp, nls_updated_at: Timestamp), so_updated_at: Timestamp))


        val dfResult = dfJoin.select(col("id_customer"),
                                     col("giftcard_credits_available"),
                                     col("store_credits_available"),
                                     col("birthday"),
                                     col("gender"),
                                     col("reward_type"),
                                     col("email"),
                                     col("created_at"),
                                     col("updated_at"),

                                     udfAccRegDate(dfJoin("created_at"),
                                                   dfJoin("nls_created_at")) as "acc_reg_date",

                                     udfMaxUpdatedAt(dfJoin("updated_at"),
                                                     dfJoin("nls_created_at"),
                                                     dfJoin("so_updated_at")) as "max_updated_at")

//          //Name of variable: id_customer, ACC_REG_DATE, UPDATED_AT
//          val dfAccRegDateAndUpdatedAt = getAccRegDateAndUpdatedAt(dfCustomer: DataFrame,
//                                                                   dfNLS: DataFrame,
//                                                                   dfSalesOrder: DataFrame)

          //Name of variable: id_customer, EMAIL_OPT_IN_STATUS
          val dfEmailOptInStatus = getEmailOptInStatus(dfCustomer: DataFrame, dfNLS: DataFrame)

          //Name of variable: id_customer, CUSTOMERS PREFERRED ORDER TIMESLOT
          val dfCPOT = getCustomersPreferredOrderTimeslot(dfSalesOrder: DataFrame)

          /*        Name of variables:id_customer,
                                    GIFTCARD_CREDITS_AVAILABLE,
                                    STORE_CREDITS_AVAILABLE,
                                    EMAIL,
                                    BIRTHDAY,
                                    GENDER,
                                    PLATINUM_STATUS*/
//          val customer = dfCustomer.select("id_customer",
//                                           "giftcard_credits_available",
//                                           "store_credits_available",
//                                           "email",
//                                           "birthday",
//                                           "gender",
//                                           "reward_type")

        dfResult.limit(10).collect().foreach(println)
        dfResult.printSchema()

         return dfJoin
      }

      def min(t1: Timestamp, t2: Timestamp): Timestamp ={

          if(t1==null){
            return t2
          }

          if(t2==null){
            return t1
          }

          if (t1.compareTo(t2) >= 0)
            t1
          else
            t2

      }

      def max(t1: Timestamp, t2: Timestamp): Timestamp ={

          if(t1==null){
            return t2
          }

          if(t2==null){
            return t1
          }

          if (t1.compareTo(t2) >= 0)
            t2
          else
            t1

      }
       //iou - i: opt in(subscribed), o: opt out(when registering they have opted out), u: unsubscribed
       def getEmailOptInStatus(dfCustomer: DataFrame, dfNLS: DataFrame): DataFrame = {

           val customer = dfCustomer.select("id_customer", "email")

           val nls = dfNLS.select("email", "status")

           val dfJoin = customer.join(nls.select("email", "status"), customer("email") === nls("email"), "left")
                                .select("id_customer", "status")

           val dfMap = dfJoin.map(e=> Row(e(0),  getStatusValue(e)))

           // Apply the schema to the RDD.
           val dfEmailOptInStatus = Spark.getSqlContext().createDataFrame(dfMap, email_opt_in_status)

           return dfEmailOptInStatus
       }

       def getStatusValue(e: Row): String = {
           if(e(1)==null){
             return "o"
           }
             e(1) match {
               case "subscribed" => return "iou"
               case "unsubscribed" => return "u"
             }
       }

       //CustomersPreferredOrderTimeslot: Time slot: 2 hrs each, start from 7 am. total 12 slots (1 to 12)
       def getCustomersPreferredOrderTimeslot(dfSalesOrder: DataFrame): DataFrame = {

           val salesOrder = dfSalesOrder.select("fk_customer", "created_at").sort("fk_customer", "created_at")

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

           for( i  <- 1 to array.length-1){

             if(i==1){
               arrayConverted=array(i).toString
             }
             else{
               arrayConverted= arrayConverted+"!"+array(i).toString
             }


           }
           arrayConverted
       }

 }
