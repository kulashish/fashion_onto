package com.jabong.dap.common

import java.math.BigDecimal

import com.jabong.dap.campaign.utils.CampaignUtils._
import com.jabong.dap.common.time.TimeUtils
import grizzled.slf4j.Logging
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.{DoubleType, IntegerType, DataType, StructType}

/**
 * Created by mubarak on 21/10/15.
 */
object Utils extends Logging {

  /**
   * Filtered Data based on before time to after Time yyyy-mm-dd HH:MM:SS.s
   * @param inData
   * @param timeField
   * @param after
   * @param before
   * @return
   */
  def getTimeBasedDataFrame(inData: DataFrame, timeField: String, after: String, before: String): DataFrame = {
    if (inData == null || timeField == null || before == null || after == null) {
      logger.error("Any of the value in getTimeBasedDataFrame is null")
      return null
    }

    if (after.length != before.length) {
      logger.error("before and after time formats are different ")
      return null
    }

    val Columns = inData.columns
    if (!(Columns contains (timeField))) {
      logger.error(timeField + "doesn't exist in the inData Frame Schema")
      return null
    }

    val filteredData = inData.filter(timeField + " >= '" + after + "' and " + timeField + " <= '" + before + "'")
    logger.info("Input Data Frame has been filtered before" + before + " after '" + after)
    return filteredData
  }

  def getOneDayData(inData: DataFrame, timeField: String, date: String, dateFormat: String): DataFrame = {
    if (inData == null || timeField == null || date == null) {
      logger.error("Any of the value in getTimeBasedDataFrame is null")
      return null
    }

    val start = TimeUtils.getStartTimestampMS(TimeUtils.getTimeStamp(date, dateFormat))

    val end = TimeUtils.getEndTimestampMS(TimeUtils.getTimeStamp(date, dateFormat))

    val Columns = inData.columns
    if (!(Columns contains (timeField))) {
      logger.error(timeField + "doesn't exist in the inData Frame Schema")
      return null
    }

    val filteredData = inData.filter(timeField + " >= '" + start + "' and " + timeField + " <= '" + end + "'")
    logger.info("Input Data Frame has been filtered before" + start + " after '" + end)
    return filteredData
  }

  /**
   * Common order group By function takes dataFrame and needed fields and return final dataFrame
   * @param inputData
   * @param groupedFields
   * @param aggFields
   * @param aggFunction
   * @param outputSchema
   * @param orderField
   * @param order
   * @param orderFieldType
   * @return
   */
  def orderGroupBy(inputData : DataFrame, groupedFields: Array[String], aggFields : Array[String], aggFunction : String, outputSchema: StructType , orderField : String,order:String = "ASC", orderFieldType:DataType): DataFrame ={
    require(inputData != null, "inputData data cannot be null ")
    require(groupedFields != null, "groupedFields  cannot be null ")
    require(aggFields != null, "aggFields cannot be null ")
    require(outputSchema != null, "outputSchema cannot be null ")
    val keyRdd = inputData.rdd.keyBy(row => createKey(row, groupedFields))
    val aggData =  keyRdd.groupByKey().map{ case (key, value) => (key, orderBySupporter(value,orderField,order,orderFieldType)) }.map{case (key,value) => (key,aggregateSupporter(value,aggFields,aggFunction))}
    val finalRow = aggData.map{case (key,value) => (Row(key.toSeq ++value.toSeq))}
    val orderGroupedData = sqlContext.createDataFrame(finalRow,outputSchema)
    return orderGroupedData
  }

  /**
   *
   * @param list
   * @param aggFields
   * @param aggFunction
   * @return
   */
  def aggregateSupporter(list: List[Row], aggFields:Array[String], aggFunction:String): Row ={
    var outRow:Row = null
    aggFunction match {
      case "first" => outRow = list(0)
      case "last"  => outRow = list(list.size-1)
    }
    return createKey(outRow,aggFields)
  }

  /**
   *
   * @param iterable
   * @param orderField
   * @param order
   * @param orderFieldDataType
   * @return
   */
  def orderBySupporter(iterable: Iterable[Row],orderField : String,order:String, orderFieldDataType : DataType): List[Row] ={
    require(iterable != null, "iterable data cannot be null ")
    require(orderField != null, "orderField  cannot be null ")
    if(orderField.toLowerCase.contains("price")){
      var ordering:Ordering[Double] = null
      if(order.equals("ASC")) ordering = Ordering.Double else ordering  = Ordering.Double.reverse
      return iterable.toList.sortBy(row => (row.fieldIndex(orderField).asInstanceOf[BigDecimal].doubleValue()))(ordering)
    }
    orderFieldDataType match {
      case IntegerType =>  var ordering:Ordering[Int] = null
        if(order.equals("ASC")) ordering = Ordering.Int else ordering  = Ordering.Int.reverse
        return iterable.toList.sortBy(row => (row.fieldIndex(orderField)))(ordering)

      case DoubleType => var ordering:Ordering[Double] = null
        if(order.equals("ASC")) ordering = Ordering.Double else ordering  = Ordering.Double.reverse
        return iterable.toList.sortBy(row => (row.fieldIndex(orderField).asInstanceOf[Double]))(ordering)
    }
  }

  /*
Given a row  and fields in that row it will return new row with only those keys
input:- row  and fields: field array
@returns row with only those fields
*/
  def createKey(row: Row, fields: Array[String]): Row = {
    if (row == null || fields == null || fields.length == 0) {
      return null
    }
    var sequence: Seq[Any] = Seq()
    for (field <- fields) {
      try {
        sequence = sequence :+ (row(row.fieldIndex(field)))
      } catch {
        case ex: IllegalArgumentException => {
          ex.printStackTrace()
          return null
        }

      }
    }
    val data = Row.fromSeq(sequence)
    return data
  }


}
