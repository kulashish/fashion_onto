package oneTimeScripts

import java.io.File

import com.jabong.dap.common.constants.variables.PageVisitVariables
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.model.customer.data.CustomerDeviceMapping
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Program to get all the ad4pushId firstTime.
 */
object loadAd4pushId {
  def main(args: Array[String]) = {
    val incrDate = args(0).trim()
    val dt = TimeUtils.getMonthAndYear(incrDate, TimeConstants.DATE_FORMAT)
    val date = incrDate + "-24"
    val hiveContext = new HiveContext(new SparkContext(new SparkConf().setAppName("gettingAd4PushIds")))
    val tablename = "clickstream.apps_pagevisit"
    val sqlQuery = "select bid as %s, %s, %s, %s from %s where date1<=%s and month1<=%s and year1<=%s"
      .format(PageVisitVariables.BROWSER_ID, PageVisitVariables.DOMAIN, PageVisitVariables.ADD4PUSH, PageVisitVariables.PAGE_TIMESTAMP, tablename, dt.day, dt.month + 1, dt.year)
    println(sqlQuery)
    val clickIncr = hiveContext.sql(sqlQuery)
    val ad4pushFull = CustomerDeviceMapping.getAd4pushId(null,clickIncr)
    val ad4pushCurPath = "%s/%s/%s/%s/%s".format("/data/output", "extras", "ad4pushId", "full", date.replaceAll("-", File.separator))
    println("Writing to path: " + ad4pushCurPath)
    ad4pushFull.write.mode(SaveMode.Overwrite).parquet(ad4pushCurPath)
    println("Written succesfully")
  }
}