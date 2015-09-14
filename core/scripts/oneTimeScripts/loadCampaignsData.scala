import java.util.Properties
import java.util.Calendar
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object loadCampaignsData {

  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val dateFolder = new java.text.SimpleDateFormat("yyyy/MM/dd")

  def main(args: Array[String]) {
    //val startDate="2015-09-03"
    val startDate = args(0).trim
    //val endDate="2015-09-09"
    val endDate = args(1).trim
    //val target_table_name="mobile_push_campaign_quality"
    val target_table_name = args(2).trim

    var temp = startDate
    val dates = scala.collection.mutable.ArrayBuffer.empty[String]

    val cal = Calendar.getInstance()

    while (temp <= endDate) {
      dates += temp

      cal.setTime(dateFormat.parse(temp))
      cal.add(Calendar.DAY_OF_YEAR, 1)
      temp = dateFormat.format(cal.getTime)

    }

    for (d <- dates) {
      insertIntoTable(d, target_table_name)
    }
  }

  def insertIntoTable(date: String, target_table_name: String) = {

    val sc = new SparkContext(new SparkConf().setAppName("LoadCamapignsData"))
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.load("/data/output/campaigns/mobile_push_campaign_quality/daily/" + dateFolder.format(dateFormat.parse(date))).withColumn("date", lit(date))
    val connProp = new Properties()
    connProp.put("user", "jdare")
    connProp.put("password", "jdare")
    df.write.mode("append").jdbc("jdbc:mysql://172.16.84.37:3306/jdare", target_table_name, connProp)
  }


}