package oneTimeScripts

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{ DataFrame, SaveMode }
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Program to get all the ad4pushId firstTime.
 */
object loadAd4pushId {
  val BROWSER_ID = "browserid"
  val ADD4PUSH = "add4push"
  val PAGE_TIMESTAMP = "pagets"

  def main(args: Array[String]) = {
    val date = args(0).trim() + "-24"
    val hiveContext = new HiveContext(new SparkContext(new SparkConf().setAppName("gettingAd4PushIds")))
    val tablename = "clickstream.apps_pagevisit"
    val sqlQuery = "select bid as %s, %s, %s from %s where domain = 'android'".format(BROWSER_ID, ADD4PUSH, PAGE_TIMESTAMP, tablename)
    val clickIncr = hiveContext.sql(sqlQuery)
    val ad4pushFull = getAd4pushId(clickIncr)
    val ad4pushCurPath = "%s/%s/%s/%s/%s".format("/data/output", "extras", "ad4pushId", "full", date.replaceAll("-", File.separator))
    println("Writing to path: " + ad4pushCurPath)
    ad4pushFull.write.mode(SaveMode.Overwrite).parquet(ad4pushCurPath)
    println("Written succesfully")
  }

  def getAd4pushId(clickstreamIncr: DataFrame): DataFrame = {
    val notNullAdd4push = clickstreamIncr
      .select(
        BROWSER_ID,
        ADD4PUSH,
        PAGE_TIMESTAMP
      )
      .dropDuplicates()
      .na.drop(Array(ADD4PUSH))
    val grouped = notNullAdd4push.orderBy(col(BROWSER_ID), desc(PAGE_TIMESTAMP))
      .groupBy(BROWSER_ID)
      .agg(
        first(ADD4PUSH) as ADD4PUSH,
        first(PAGE_TIMESTAMP) as PAGE_TIMESTAMP
      )
    grouped
  }
}