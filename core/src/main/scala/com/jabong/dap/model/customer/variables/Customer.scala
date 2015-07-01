package com.jabong.dap.model.customer.variables

import java.sql.Timestamp

import com.jabong.dap.common.constants.variables.{SalesOrderVariables, NewsletterVariables, CustomerVariables}
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

      val emailOptInStatus = StructType(Array(StructField(CustomerVariables.IdCustomer, IntegerType, true),
                                                 StructField(NewsletterVariables.Status, StringType, true)))

      val accRegDateAndUpdatedAt = StructType(Array(StructField(CustomerVariables.Email, StringType, true),
                                                    StructField(CustomerVariables.AccRegDate, TimestampType, true),
                                                    StructField(CustomerVariables.UpdatedAt, TimestampType, true)))

      val customersPreferredOrderTimeslot = StructType(Array(StructField(CustomerVariables.FkCustomerCPOT, IntegerType, true),
                                                                StructField(CustomerVariables.CustomerAllOrderTimeslot, StringType, true),
                                                                StructField(CustomerVariables.CustomerPreferredOrderTimeslot, IntegerType, true)))

      val resultCustomer =  StructType(Array(StructField(CustomerVariables.IdCustomer, IntegerType, true),
                                              StructField(CustomerVariables.GiftcardCreditsAvailable, DecimalType(10,2), true),
                                              StructField(CustomerVariables.StoreCreditsAvailable, DecimalType(10,2), true),
                                              StructField(CustomerVariables.Birthday, DateType, true),
                                              StructField(CustomerVariables.Gender, StringType, true),
                                              StructField(CustomerVariables.RewardType, StringType, true),
                                              StructField(CustomerVariables.Email, StringType, true),
                                              StructField(CustomerVariables.CreatedAt, TimestampType, true),
                                              StructField(CustomerVariables.UpdatedAt, TimestampType, true),
                                              StructField(CustomerVariables.CustomerAllOrderTimeslot, StringType, true),
                                              StructField(CustomerVariables.CustomerPreferredOrderTimeslot, IntegerType, true),
                                              StructField(CustomerVariables.AccRegDate, TimestampType, true),
                                              StructField(CustomerVariables.MaxUpdatedAt, TimestampType, true),
                                              StructField(CustomerVariables.EmailOptInStatus, StringType, true)))
    
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


          val NLS = dfNLS.select(col(NewsletterVariables.Email) as NewsletterVariables.NlsEmail,
                                 col(NewsletterVariables.Status),
                                 col(NewsletterVariables.CreatedAt) as NewsletterVariables.NlsCreatedAt,
                                 col(NewsletterVariables.UpdatedAt) as NewsletterVariables.NlsUpdatedAt)

          //Name of variable: CUSTOMERS PREFERRED ORDER TIMESLOT
          val udfCPOT = getCPOT(dfSalesOrder: DataFrame)

          val dfJoin = dfCustomer.select(CustomerVariables.IdCustomer,
                                         CustomerVariables.GiftcardCreditsAvailable,
                                         CustomerVariables.StoreCreditsAvailable,
                                         CustomerVariables.Birthday,
                                         CustomerVariables.Gender,
                                         CustomerVariables.RewardType,
                                         CustomerVariables.Email,
                                         CustomerVariables.CreatedAt,
                                         CustomerVariables.UpdatedAt)

                                        .join(NLS, dfCustomer(CustomerVariables.Email) === NLS(NewsletterVariables.NlsEmail), "outer")

                                        .join(dfSalesOrder.select(col(SalesOrderVariables.FkCustomer),
                                                                  col(SalesOrderVariables.CreatedAt) as SalesOrderVariables.SoCreatedAt,
                                                                  col(SalesOrderVariables.UpdatedAt) as SalesOrderVariables.SoUpdatedAt),
                                         dfCustomer(CustomerVariables.IdCustomer) === dfSalesOrder(SalesOrderVariables.FkCustomer), "outer")

                                        .join(udfCPOT, dfCustomer(CustomerVariables.IdCustomer) === udfCPOT(CustomerVariables.FkCustomerCPOT), "outer")


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
          val dfResult = dfJoin.select(col(CustomerVariables.IdCustomer),
                                       col(CustomerVariables.GiftcardCreditsAvailable),
                                       col(CustomerVariables.StoreCreditsAvailable),
                                       col(CustomerVariables.Birthday),
                                       col(CustomerVariables.Gender),
                                       col(CustomerVariables.RewardType),
                                       col(CustomerVariables.Email),
                                       col(CustomerVariables.CreatedAt),
                                       col(CustomerVariables.UpdatedAt),
                                       col(CustomerVariables.CustomerAllOrderTimeslot),
                                       col(CustomerVariables.CustomerPreferredOrderTimeslot),

                                       udfAccRegDate(dfJoin(CustomerVariables.CreatedAt),
                                                     dfJoin(NewsletterVariables.NlsCreatedAt)) as CustomerVariables.AccRegDate,

                                       udfMaxUpdatedAt(dfJoin(CustomerVariables.UpdatedAt),
                                                       dfJoin(NewsletterVariables.NlsCreatedAt),
                                                       dfJoin(SalesOrderVariables.SoCreatedAt)) as CustomerVariables.MaxUpdatedAt,

                                       udfEmailOptInStatus(dfJoin(NewsletterVariables.NlsEmail),
                                                           dfJoin(NewsletterVariables.Status)) as CustomerVariables.EmailOptInStatus)

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
             return "O"
           }

           status match {
             case "subscribed" => "I"
             case "unsubscribed" => "U"
           }

       }

       //CustomersPreferredOrderTimeslot: Time slot: 2 hrs each, start from 7 am. total 12 slots (1 to 12)
       def getCPOT(dfSalesOrder: DataFrame): DataFrame = {

           val salesOrder = dfSalesOrder.select(SalesOrderVariables.FkCustomer, SalesOrderVariables.CreatedAt)
                                        .sort(SalesOrderVariables.FkCustomer, SalesOrderVariables.CreatedAt)

           val soMapReduce=salesOrder.map(r=> ((r(0), Time.timeToSlot(r(1).toString, Constants.DATETIME_FORMAT)),1)).reduceByKey(_+_)

           val soNewMap = soMapReduce.map{case(key,value)=>(key._1,(key._2.asInstanceOf[Int],value.toInt))}

           val soGrouped = soNewMap.groupByKey()

           val finalData =  soGrouped.map{case(key,value)=> (key.toString, getCompleteSlotData(value))}

           val rowRDD = finalData.map({case(key,value) => Row(key.toInt, value._1, value._2)})

           // Apply the schema to the RDD.
           val df = Spark.getSqlContext().createDataFrame(rowRDD, customersPreferredOrderTimeslot)

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
