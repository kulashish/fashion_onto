package com.jabong.dap.model.clickstream.variables

import com.jabong.dap.model.clickstream.utils.GroupData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by jabong on 15/7/15.
 */
object GetSurfVariables extends java.io.Serializable {

  def Surf3(GroupedData: RDD[(String, Row)], UserObj: GroupData, hiveContext: HiveContext): RDD[(String, (Any, Any, Any))] = {
    val dailyIncremental = GroupedData.filter(v => v._2(UserObj.pagetype) == "CPD" || v._2(UserObj.pagetype) == "DPD" || v._2(UserObj.pagetype) == "QPD")
      .mapValues(x => (x(UserObj.productsku), x(UserObj.browserid), x(UserObj.domain)))
    val prepareForMerge1 = dailyIncremental.mapValues(x => x._1)
    val prepareForMerge2 = prepareForMerge1.reduceByKey((x, y) => (x + "," + y))
    return dailyIncremental
  }

}
