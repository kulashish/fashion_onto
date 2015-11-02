package com.jabong.dap.common

import java.math.BigDecimal

import com.jabong.dap.campaign.utils.CampaignUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.types._

/**
 * Created by rahul on 29/10/15.
 */
object GroupedUtils {

  val FIRST = "first"
  val LAST = "last"
  val ASC = "ASC"
  val DESC = "DESC"

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
  def orderGroupBy(inputData: DataFrame, groupedFields: Array[String], aggFields: Array[String], aggFunction: String, outputSchema: StructType, orderField: String, order: String = "ASC", orderFieldType: DataType): DataFrame = {
    require(inputData != null, "inputData data cannot be null ")
    require(groupedFields != null, "groupedFields  cannot be null ")
    require(aggFields != null, "aggFields cannot be null ")
    require(outputSchema != null, "outputSchema cannot be null ")
    val keyRdd = inputData.rdd.keyBy(row => Utils.createKey(row, groupedFields))
    val aggData = keyRdd.groupByKey().map{ case (key, value) => (key, orderBySupporter(value, orderField, order, orderFieldType)) }.map{ case (key, value) => (key, aggregateSupporter(value, aggFields, aggFunction)) }
    val finalRow = aggData.map{ case (key, value) => (value) }
    val orderGroupedData = sqlContext.createDataFrame(finalRow, outputSchema)
    return orderGroupedData
  }

  /**
   *
   * @param list
   * @param aggFields
   * @param aggFunction
   * @return
   */
  def aggregateSupporter(list: List[Row], aggFields: Array[String], aggFunction: String): Row = {
    var outRow: Row = null
    aggFunction match {
      case FIRST => outRow = list(0)
      case LAST => outRow = list(list.size - 1)
    }
    return Utils.createKey(outRow, aggFields)
  }

  /**
   *
   * @param iterable
   * @param orderField
   * @param order
   * @param orderFieldDataType
   * @return
   */
  def orderBySupporter(iterable: Iterable[Row], orderField: String, order: String, orderFieldDataType: DataType): List[Row] = {
    require(iterable != null, "iterable data cannot be null ")
    require(orderField != null, "orderField  cannot be null ")

    orderFieldDataType match {
      case IntegerType =>
        var ordering: Ordering[Int] = null
        if (order.equals(ASC)) ordering = Ordering.Int else ordering = Ordering.Int.reverse
        return iterable.toList.sortBy(row => (row(row.fieldIndex(orderField))).asInstanceOf[Int])(ordering)

      case DoubleType =>
        var ordering: Ordering[Double] = null
        if (order.equals(ASC)) ordering = Ordering.Double else ordering = Ordering.Double.reverse
        return iterable.toList.sortBy(row => (row(row.fieldIndex(orderField)).asInstanceOf[Double]))(ordering)

      case DecimalType() =>
        var ordering: Ordering[Double] = null
        if (order.equals(ASC)) ordering = Ordering.Double else ordering = Ordering.Double.reverse
        return iterable.toList.sortBy(row => (row(row.fieldIndex(orderField)).asInstanceOf[BigDecimal].doubleValue()))(ordering)

      case StringType =>
        var ordering: Ordering[String] = null
        if (order.equals(ASC)) ordering = Ordering.String else ordering = Ordering.String.reverse
        return iterable.toList.sortBy(row => (row(row.fieldIndex(orderField))).asInstanceOf[String])(ordering)
    }
  }
}
