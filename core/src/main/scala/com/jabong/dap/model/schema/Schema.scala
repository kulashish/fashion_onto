package com.jabong.dap.model.schema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 22/6/15.
 */
object Schema {

      val customer = StructType(Array(StructField("id_customer", IntegerType , true),
                                      StructField("email", StringType, true),
                                      StructField("increment_id", StringType, true),
                                      StructField("prefix", StringType, true),
                                      StructField("first_name", StringType, true),
                                      StructField("middle_name", StringType, true),
                                      StructField("last_name", StringType, true),
                                      StructField("birthday", DateType, true),
                                      StructField("gender", StringType, true),
                                      StructField("phone", StringType, true),
                                      StructField("password", StringType, true),
                                      StructField("restore_password_key", StringType, true),
                                      StructField("is_confirmed", BooleanType, true),
                                      StructField("email_verified_at", TimestampType, true),
                                      StructField("is_mobile_verified", BooleanType, true),
                                      StructField("mobile_verified_at", TimestampType, true),
                                      StructField("confirmation_key", StringType, true),
                                      StructField("payment_reference_key", StringType, true),
                                      StructField("created_at", TimestampType, true),
                                      StructField("updated_at", TimestampType, true),
                                      StructField("store_credits_available", DecimalType(10,2), true),
                                      StructField("store_credits_validity", DateType, true),
                                      StructField("giftcard_credits_available", DecimalType(10,2), true),
                                      StructField("giftcard_credits_validity", DateType, true),
                                      StructField("session_cookie", StringType, true),
                                      StructField("logged_in", TimestampType, true),
                                      StructField("reward_type", StringType, true),
                                      StructField("domain", StringType, true),
                                      StructField("user_device_type", StringType, true),
                                      StructField("api_token", StringType, true),
                                      StructField("app_logged_in", TimestampType, true),
                                      StructField("id_customer_additional_info", IntegerType , true),
                                      StructField("fk_customer", IntegerType , true),
                                      StructField("source", StringType, true),
                                      StructField("city", StringType, true),
                                      StructField("relationship_status", StringType, true),
                                      StructField("anniversary_date", DateType, true),
                                      StructField("facebook_uid", StringType, true),
                                      StructField("facebook_pic_url", StringType, true),
                                      StructField("facebook_linked_date", TimestampType, true),
                                      StructField("is_via_fbconnect", BooleanType, true),
                                      StructField("calculated_gender", StringType, true),
                                      StructField("google_uid", StringType, true),
                                      StructField("google_pic_url", StringType, true),
                                      StructField("google_linked_date", TimestampType, true),
                                      StructField("is_via_googleconnect", BooleanType, true),
                                      StructField("google_refresh_token", StringType, true),
                                      StructField("fb_access_token", StringType, true),
                                      StructField("app_version", StringType, true),
                                      StructField("fk_corporate_customer", IntegerType , true),
                                      StructField("fk_referral_code", IntegerType , true),
                                      StructField("sms_opt", BooleanType, true)))

      val nls = StructType(Array(StructField("id_newsletter_subscription", IntegerType , true),
                                 StructField("fk_customer", IntegerType , true),
                                 StructField("email", StringType, true),
                                 StructField("unsubscribe_key", StringType, true),
                                 StructField("ip", StringType, true),
                                 StructField("created_at", TimestampType, true),
                                 StructField("status", StringType, true),
                                 StructField("gender", StringType, true),
                                 StructField("updated_at", TimestampType, true),
                                 StructField("fk_newsletter_category", IntegerType , true),
                                 StructField("newsletter_preferences", StringType, true),
                                 StructField("fk_affiliate_partner", IntegerType , true),
                                 StructField("src_sub", StringType, true),
                                 StructField("src_unsub", StringType, true),
                                 StructField("frequency", StringType, true)))

      val salesOrder = StructType(Array(StructField("id_sales_order", IntegerType , true),
                                        StructField("fk_sales_order_address_billing", IntegerType , true),
                                        StructField("fk_sales_order_address_shipping", IntegerType , true),
                                        StructField("fk_customer", IntegerType , true),
                                        StructField("customer_first_name", StringType, true),
                                        StructField("customer_last_name", StringType, true),
                                        StructField("customer_email", StringType, true),
                                        StructField("order_nr", StringType, true),
                                        StructField("customer_session_id", StringType, true),
                                        StructField("store_id", IntegerType , true),
                                        StructField("grand_total", DecimalType(10,2), true),
                                        StructField("tax_amount", DecimalType(10,2), true),
                                        StructField("shipping_amount", DecimalType(10,2), true),
                                        StructField("shipping_method", StringType, true),
                                        StructField("coupon_code", StringType, true),
                                        StructField("payment_method", StringType, true),
                                        StructField("created_at", TimestampType, true),
                                        StructField("updated_at", TimestampType, true),
                                        StructField("fk_shipping_carrier", IntegerType , true),
                                        StructField("tracking_url", StringType, true),
                                        StructField("otrs_ticket", StringType, true),
                                        StructField("fk_sales_order_process", IntegerType , true),
                                        StructField("shipping_discount_amount", DecimalType(10,0), true),
                                        StructField("ip", StringType, true),
                                        StructField("invoice_file", StringType, true),
                                        StructField("invoice_nr", StringType, true),
                                        StructField("is_recurring", BooleanType, true),
                                        StructField("ccavenue_order_number", StringType, true),
                                        StructField("cod_charge", DecimalType(10,2), true),
                                        StructField("retrial", BooleanType, true),
                                        StructField("id_sales_order_additional_info", IntegerType , true),
                                        StructField("fk_sales_order", IntegerType , true),
                                        StructField("fk_affiliate_partner", IntegerType , true),
                                        StructField("fk_shipping_partner_agent", IntegerType , true),
                                        StructField("domain", StringType, true),
                                        StructField("user_device_type", StringType, true),
                                        StructField("shipment_delay_days", IntegerType , true),
                                        StructField("mobile_verification", StringType, true),
                                        StructField("address_mismatch", IntegerType , true),
                                        StructField("earn_method", StringType, true),
                                        StructField("parent_order_id", IntegerType , true),
                                        StructField("utm_campaign", StringType, true),
                                        StructField("reward_points", DecimalType(10,2), true),
                                        StructField("app_version", StringType, true),
                                        StructField("fk_corporate_customer", IntegerType , true),
                                        StructField("corporate_currency_value", DecimalType(10,2), true),
                                        StructField("corporate_transaction_id", StringType, true),
                                        StructField("device_id", StringType, true)))

      val csh = StructType(Array(StructField("id_customer_storecredits_history", IntegerType , true),
                                 StructField("fk_customer", IntegerType , true),
                                 StructField("created_at", TimestampType, true),
                                 StructField("operation_type", StringType, true),
                                 StructField("fk_operation", IntegerType , true),
                                 StructField("store_credits_value", DecimalType(10,2), true),
                                 StructField("credit_type", StringType, true),
                                 StructField("transaction_type", IntegerType , true),
                                 StructField("transaction_value", DecimalType(10,2), true),
                                 StructField("note", StringType, true),
                                 StructField("fk_acl_user", IntegerType , true),
                                 StructField("updated_at", TimestampType, true),
                                 StructField("balance", DecimalType(10,2), true),
                                 StructField("expiry_date", DateType, true)))

      val customerSegments = StructType(Array(StructField("id_customer_segments", IntegerType , true),
                                              StructField("segment", IntegerType , true),
                                              StructField("frequency", IntegerType , true),
                                              StructField("recency", IntegerType , true),
                                              StructField("mvp_score", IntegerType , true),
                                              StructField("discount_score", IntegerType , true),
                                              StructField("fk_customer", IntegerType , true),
                                              StructField("created_at", TimestampType, true),
                                              StructField("updated_at", TimestampType, true)))

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
  //customer_storecredits_history variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      val last_jr_covert_date = StructType(Array(StructField("fk_customer", IntegerType, true),
                                                 StructField("last_jr_covert_date", TimestampType, true)))


  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer_segments variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val mvp_seg = StructType(Array(StructField("fk_customer", IntegerType, true),
                                 StructField("mvp_score", IntegerType, true),
                                 StructField("segment", IntegerType, true)))



  //check two schema is Equals
      def isEquals(schemaFirst: StructType, schemaSecond: StructType): Boolean ={

            val fieldTypesFirst = schemaFirst.map(field => s"${field.name}:${field.dataType.simpleString}").toSet
            val fieldTypesSecond = schemaSecond.map(field => s"${field.name}:${field.dataType.simpleString}").toSet

            return fieldTypesFirst.equals(fieldTypesSecond)
        }


      //schema check for customer data frame
      def isCustomerSchema(schema: StructType): Boolean = {

          if(!Schema.isEquals(schema, Schema.customer)){

            log("schema attributes or data type mismatch, it should be: " + Schema.customer)

            return false
          }

          return true
      }

      //schema check for NLS data frame
      def isNLSSchema(schema: StructType): Boolean = {

          if(!Schema.isEquals(schema, Schema.nls)){

            log("schema attributes or data type mismatch, it should be: " + Schema.nls)

            return false
          }

          return true
      }

      //schema check for Sales Order data frame
      def isSalesOrderSchema(schema: StructType): Boolean = {

          if(!Schema.isEquals(schema, Schema.salesOrder)){

            log("schema attributes or data type mismatch, it should be: " + Schema.salesOrder)

            return false
          }

          return true
      }

      //schema check for CSH data frame
      def isCSHSchema(schema: StructType): Boolean = {

          if(!Schema.isEquals(schema, Schema.csh)){

            log("schema attributes or data type mismatch, it should be: " + Schema.csh)

            return false
          }

          return true
      }

      //schema check for Customer Segments data frame
      def isCustomerSegmentsSchema(schema: StructType): Boolean = {

          if(!Schema.isEquals(schema, Schema.customerSegments)){

            log("schema attributes or data type mismatch, it should be: " + Schema.customerSegments)

            return false
          }

          return true
      }


}
