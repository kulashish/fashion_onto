package com.jabong.dap.model.customer

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.{ DataReader, PathBuilder }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.customer.variables.Customer
import com.jabong.dap.model.order.variables.SalesOrderAddress
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 17/8/15.
 */
object ContactListMobile extends Logging {

  /**
   *
   * @param vars
   */
  def start(vars: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))
    val path = OptionUtils.getOptValue(vars.path)
    val saveMode = vars.saveMode

    //read Data Frames
    val (dfCustomer, dfNLS, dfSalesOrder, dfCustomerPrevFull, dfSalesOrderAddress, dfSalesOrderPrevFull) = readDf(path, saveMode, prevDate)

    //get Customer New Data Frame
    val dfCustomerNew = getCustomer(path, saveMode, incrDate, dfCustomer, dfNLS, dfSalesOrder, dfCustomerPrevFull)

    //get Sales Order New Data Frame
    val dfSalesOrderNew = getSalesOrder(path, saveMode, incrDate, dfSalesOrder, dfSalesOrderAddress, dfSalesOrderPrevFull)

    //Save Data Frame Contact List Mobile
    saveDF(path, saveMode, incrDate, dfCustomerNew, dfSalesOrderNew)

  }

  /**
   *
   * @param path
   * @param saveMode
   * @param incrDate
   * @param dfCustomerNew
   * @param dfSalesOrderNew
   */
  def saveDF(path: String, saveMode: String, incrDate: String, dfCustomerNew: DataFrame, dfSalesOrderNew: DataFrame) = {

    val dfJoin = dfCustomerNew.join(dfSalesOrderNew, dfCustomerNew(CustomerVariables.ID_CUSTOMER) === dfSalesOrderNew(SalesOrderVariables.FK_CUSTOMER))

    val dfContactListMobile = dfJoin.select(
      col(CustomerVariables.ID_CUSTOMER),
      col(CustomerVariables.GIFTCARD_CREDITS_AVAILABLE),
      col(CustomerVariables.STORE_CREDITS_AVAILABLE),
      col(CustomerVariables.BIRTHDAY),
      col(CustomerVariables.GENDER),
      col(CustomerVariables.REWARD_TYPE),
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.CREATED_AT),
      col(CustomerVariables.UPDATED_AT),
      col(CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT),
      col(CustomerVariables.CUSTOMER_PREFERRED_ORDER_TIMESLOT),
      col(CustomerVariables.FIRST_NAME),
      col(CustomerVariables.LAST_NAME),
      coalesce(col(CustomerVariables.PHONE), col(SalesAddressVariables.MOBILE)) as SalesAddressVariables.MOBILE,
      col(CustomerVariables.CITY),
      col(CustomerVariables.VERIFICATION_STATUS),
      col(NewsletterVariables.NL_SUB_DATE),
      col(NewsletterVariables.UNSUB_KEY),
      col(CustomerVariables.AGE),
      col(CustomerVariables.ACC_REG_DATE),
      col(CustomerVariables.MAX_UPDATED_AT),
      col(CustomerVariables.EMAIL_OPT_IN_STATUS),
      col(SalesAddressVariables.CITY),
      lit("IN") as SalesAddressVariables.COUNTRY,
      col(SalesAddressVariables.FIRST_NAME),
      col(SalesAddressVariables.LAST_NAME)
    )

    //save Data Frame to parquet file
    val pathWriter = PathBuilder.buildPath(path, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, saveMode, incrDate)

    dfContactListMobile.write.parquet(pathWriter)

  }

  /**
   *
   * @param path
   * @param saveMode
   * @param incrDate
   * @param dfSalesOrder
   * @param dfSalesOrderAddress
   * @param dfSalesOrderPrevFull
   * @return
   */
  def getSalesOrder(path: String, saveMode: String, incrDate: String, dfSalesOrder: DataFrame, dfSalesOrderAddress: DataFrame, dfSalesOrderPrevFull: DataFrame): DataFrame = {

    //call SalesOrderAddress.processVariable
    val (dfSalesOrderNew, dfSalesOrderNewFull) = SalesOrderAddress.processVariable(dfSalesOrder, dfSalesOrderAddress, dfSalesOrderPrevFull)
    val pathSalesOrder = PathBuilder.buildPath(path, DataSets.VARIABLES, DataSets.SALES_ORDER_FULL, saveMode, incrDate)

    dfSalesOrderNewFull.write.parquet(pathSalesOrder)

    dfSalesOrderNew
  }

  /**
   *
   * @param path
   * @param saveMode
   * @param incrDate
   * @param dfCustomer
   * @param dfNLS
   * @param dfSalesOrder
   * @param dfCustomerPrevFull
   * @return
   */
  def getCustomer(path: String, saveMode: String, incrDate: String, dfCustomer: DataFrame, dfNLS: DataFrame, dfSalesOrder: DataFrame, dfCustomerPrevFull: DataFrame): DataFrame = {

    //call Customer.getCustomer
    val (dfCustomerNew, dfCustomerFull) = Customer.getCustomer(dfCustomer, dfNLS, dfSalesOrder, dfCustomerPrevFull)
    val pathCustomer = PathBuilder.buildPath(path, DataSets.VARIABLES, DataSets.CUSTOMER_FULL, saveMode, incrDate)

    dfCustomerFull.write.parquet(pathCustomer)

    dfCustomerNew
  }

  /**
   * read Data Frames
   * @param path
   * @param saveMode
   * @param prevDate
   * @return
   */
  def readDf(path: String, saveMode: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    val dfCustomer = DataReader.getDataFrame(path, DataSets.VARIABLES, DataSets.CUSTOMER, saveMode, prevDate)
    val dfNLS = DataReader.getDataFrame(path, DataSets.VARIABLES, DataSets.NEWSLETTER_SUBSCRIPTION, saveMode, prevDate)
    val dfSalesOrder = DataReader.getDataFrame(path, DataSets.VARIABLES, DataSets.SALES_ORDER, saveMode, prevDate)
    val dfCustomerPrevFull = DataReader.getDataFrame(path, DataSets.VARIABLES, DataSets.CUSTOMER_FULL, saveMode, prevDate)
    val dfSalesOrderAddress = DataReader.getDataFrame(path, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, saveMode, prevDate)
    val dfSalesOrderPrevFull = DataReader.getDataFrame(path, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, saveMode, prevDate)

    (dfCustomer, dfNLS, dfSalesOrder, dfCustomerPrevFull, dfSalesOrderAddress, dfSalesOrderPrevFull)

  }

}
