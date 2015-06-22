package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.{Time, DataFiles, Spark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}


import scala.reflect.io.File

/**
  * Created by raghu on 27/5/15.
  */
object Customer {

 //        val sqlContext = new org.apache.spark.sql.SQLContext(Spark.sc)

   def main(args: Array[String]) {
       try{

         //get customer parquet file
         val date = File.separator + args(0).trim.replaceAll("-", File.separator) + File.separator

         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
         // DataFrame Customer operations
         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

         val dfCustomer = Spark.getSqlContext().read.parquet(DataFiles.BOB_PATH + DataFiles.CUSTOMER + date)
         val dfNLS = Spark.getSqlContext().read.parquet(DataFiles.BOB_PATH + DataFiles.NEWSLETTER_SUBSCRIPTION + date)
         val dfSalesOrder = Spark.getSqlContext().read.parquet(DataFiles.BOB_PATH + DataFiles.SALES_ORDER + date)

         //Name of variable: id_customer, ACC_REG_DATE, UPDATED_AT
         val dfAccRegDateAndUpdatedAt = getAccRegDateAndUpdatedAt(dfCustomer: DataFrame,
                                                                   dfNLS: DataFrame,
                                                                   dfSalesOrder: DataFrame)

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
         val customer = dfCustomer.select("id_customer",
                                           "giftcard_credits_available",
                                           "store_credits_available",
                                           "email",
                                           "birthday",
                                           "gender",
                                           "reward_type")

         customer.write.parquet(DataFiles.VARIABLE_PATH + DataFiles.CUSTOMER + date)


         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
         // DataFrame CUSTOMER_STORECREDITS_HISTORY operations
         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

         val dfCSH = Spark.getSqlContext().read.parquet(DataFiles.BOB_PATH + DataFiles.CUSTOMER_STORECREDITS_HISTORY + date)

         //Name of variable: fk_customer, LAST_JR_COVERT_DATE
         val dfLastJrCovertDate = getLastJrCovertDate(dfCSH)

         dfLastJrCovertDate.write.parquet(DataFiles.VARIABLE_PATH + DataFiles.CUSTOMER_STORECREDITS_HISTORY + date)


         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
         // DataFrame CUSTOMER_SEGMENTS operations
         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

         val dfCustomerSegments = Spark.getSqlContext().read.parquet(DataFiles.BOB_PATH + DataFiles.CUSTOMER_SEGMENTS + date)

         //Name of variable: fk_customer, MVP, Segment0, Segment1,Segment2, Segment3, Segment4, Segment5, Segment6
         val dfCustSegVars = getMvpAndSeg(dfCustomerSegments)

         dfCustSegVars.write.parquet(DataFiles.VARIABLE_PATH + DataFiles.CUSTOMER_SEGMENTS + date)

       }catch {
         case e => e.printStackTrace
       }
   }


   //min(customer.created_at, newsletter_subscription.created_at, sales_order.created_at)
   // AND max(customer.updated_at, newsletter_subscription.updated_at, sales_order.updated_at)
   def getAccRegDateAndUpdatedAt(dfCustomer: DataFrame, dfNLS: DataFrame, dfSalesOrder: DataFrame): DataFrame = {

       if(dfCustomer == null || dfNLS == null || dfSalesOrder == null ){

          log("Data frame should not be null")

          return null

       }

       val customer = dfCustomer.select("email", "created_at", "updated_at")

       val nls = dfNLS.select("email", "created_at", "updated_at")

       val so = dfSalesOrder.select("customer_email", "created_at", "updated_at")
                            .withColumnRenamed("customer_email", "email")

       val dfCustNlsSO = customer.unionAll(nls).unionAll(so)

       val dfAccRegDateAndUpdatedAt = dfCustNlsSO.groupBy("email")
                                                 .agg(min(dfCustNlsSO("created_at")) as "acc_reg_date",
                                                       max(dfCustNlsSO("updated_at")) as "updated_at")

       dfAccRegDateAndUpdatedAt
   }


   //iou - i: opt in(subscribed), o: opt out(when registering they have opted out), u: unsubscribed
   def getEmailOptInStatus(dfCustomer: DataFrame, dfNLS: DataFrame): DataFrame = {

       val customer = dfCustomer.select("id_customer", "email")

       val nls = dfNLS.select("email", "status")

       val dfJoin = customer.join(nls.select("email", "status"), customer("email") === nls("email"), "left")
                            .select("id_customer", "status")

       val dfMap = dfJoin.map(e=> e(0) + ","  +  getStatusValue(e))

       val schemaString = "id_customer1 status"

       // Generate the schema based on the string of schema
       val schema = StructType(schemaString.split(" ")
                                           .map(fieldName => StructField(fieldName, StringType, true)))

       // Convert records of the RDD (segments) to Rows.
       val rowRDD = dfMap.map(_.split(",")).map(r => Row(r(0).trim, r(1).trim))


       // Apply the schema to the RDD.
       val toInt = udf[Int, String](_.toInt)

       // Apply the schema to the RDD.
       val dfs = Spark.getSqlContext().createDataFrame(rowRDD, schema)

       val dfEmailOptInStatus = dfs.withColumn("id_customer", toInt(dfs("id_customer1")))
         .select("id_customer", "status")

       dfEmailOptInStatus
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
   def  getCustomersPreferredOrderTimeslot(dfSalesOrder: DataFrame): DataFrame = {

         val salesOrder = dfSalesOrder.select("fk_customer", "created_at").sort("fk_customer", "created_at")

         val soMapReduce=salesOrder.map(r=> ((r(0), Time.timeToSlot(r(1).toString)),1)).reduceByKey(_+_)

         val soNewMap = soMapReduce.map{case(key,value)=>(key._1,(key._2.asInstanceOf[Int],value.toInt))}

         val soGrouped = soNewMap.groupByKey()

         val finalData =  soGrouped.map{case(key,value)=> (key.toString, getCompleteSlotData(value))}

         val rowRDD = finalData.map({case(key,value) => Row(key,value._1,value._2)})

         val schema = StructType(Array(StructField("fk_customer", StringType, true),
                                       StructField("customer_all_order_timeslot",StringType,true),
                                       StructField("customer_preferred_order_timeslot", IntegerType,true)))

         // Apply the schema to the RDD.
         val df = Spark.getSqlContext().createDataFrame(rowRDD, schema)

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


   //customer_storecredits_history.operation_type = "nextbee_points_added", latest date for fk_customer
   def getLastJrCovertDate(dfCSH: DataFrame): DataFrame = {

       val dfLastJrCovertDate = dfCSH.select("fk_customer", "created_at")
                                     .groupBy("fk_customer")
                                     .agg(max("created_at") as "last_jr_covert_date")

       dfLastJrCovertDate
   }

   //calculate mvp_score and  latest updated value of mvp_score from customer_segments
   def getMvpAndSeg(dfCustomerSegments: DataFrame): DataFrame = {

       val dfCustSegVars = dfCustomerSegments.select("fk_customer", "updated_at", "mvp_score", "segment")
                                             .sort(col("fk_customer"), desc("updated_at"))
                                             .groupBy("fk_customer")
                                             .agg(first("mvp_score") as "mvp_score",
                                                   first("segment") as "segment")

   //    val segments = getSeg(dfCustSegVars)

       dfCustSegVars
   }

   def getSeg(dfCustSegVars: DataFrame): DataFrame = {

       val segments = dfCustSegVars.map(r => r(0) + "," + r(1) + "," + getSegValue(r(2).toString))

       val schemaString = "fk_customer1 mvp_score segment0 segment1 segment2 segment3 segment4 segment5 segment6"

       // Generate the schema based on the string of schema
       val schema = StructType(schemaString.split(" ")
                                           .map(fieldName => StructField(fieldName, StringType, true)))

       // Convert records of the RDD (segments) to Rows.
       val rowRDD = segments.map(_.split(","))
                            .map(r => Row(r(0).trim,
                                          r(1).trim,
                                          r(2).trim,
                                          r(3).trim,
                                          r(4).trim,
                                          r(5).trim,
                                          r(6).trim,
                                          r(7).trim,
                                          r(8).trim))

       // Apply the schema to the RDD.
       val toInt = udf[Int, String](_.toInt)

       // Apply the schema to the RDD.
       val dfs = Spark.getSqlContext().createDataFrame(rowRDD, schema)

       val dfs1 = dfs.withColumn("fk_customer", toInt(dfs("fk_customer1")))
                     .select("fk_customer",
                             "mvp_score",
                             "segment0",
                             "segment1",
                             "segment2",
                             "segment3",
                             "segment4",
                             "segment5",
                             "segment6")
       dfs1
   }
   def getSegValue(i: String): String = {
       val x = Integer.parseInt(i)
       x match {
         case 0 => return "YES,NO,NO,NO,NO,NO,NO"
         case 1 => return "NO,YES,NO,NO,NO,NO,NO"
         case 2 => return "NO,NO,YES,NO,NO,NO,NO"
         case 3 => return "NO,NO,NO,YES,NO,NO,NO"
         case 4 => return "NO,NO,NO,NO,YES,NO,NO"
         case 5 => return "NO,NO,NO,NO,NO,YES,NO"
         case 6 => return "NO,NO,NO,NO,NO,NO,YES"
       }
       "NO,NO,NO,NO,NO,NO,NO"
   }


 }
