package com.jabong.dap.export.mongo

import com.jabong.dap.common.{Spark, OptionUtils}
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging
import org.apache.spark.sql.SaveMode

/**
 * Created by rahul on 22/12/15.
 */
object MongoFeedGenerator  extends Logging {
  val sqlContext = Spark.getSqlContext()
  /**
   * Starting point of mongo feed generation
   * @param params
   */
  def   start(params: ParamInfo) {
    logger.info("mongo feed generation process started")
    val executeDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val options = Map("host" -> "172.16.90.53:27017", "credentials" ->"reco,reco,xD8b6jG72", "database" -> "reco", "collection" -> "poc")
    val brickMvpSearchRecommendations = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH,DataSets.RECOMMENDATIONS,DataSets.BRICK_MVP_SEARCH_RECOMMENDATIONS,DataSets.DAILY_MODE,executeDate)
    // df4.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map( "table" -> "poc", "keyspace" -> "reco")).save()
    brickMvpSearchRecommendations.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(options).save()
    val df = sqlContext.read.format("com.stratio.datasource.mongodb").options(options).load
    df.show(100)
  }
}
