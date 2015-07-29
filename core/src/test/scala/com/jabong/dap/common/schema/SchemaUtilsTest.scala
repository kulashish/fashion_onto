package com.jabong.dap.common.schema

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.schema.Schema
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.DataFrame
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by pooja on 28/7/15.
 */
class SchemaUtilsTest extends FlatSpec with Matchers with Serializable with SharedSparkContext {
  val jsonPath: String = "src/test/resources/campaign/campaign_config/push_campaign_conf.json"
  val conf1 = new Configuration()
  val fileSystem = FileSystem.get(conf1)
  implicit val formats = net.liftweb.json.DefaultFormats
  val path = new Path(jsonPath)
  val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
  @transient var campaignsOutData: DataFrame = _


  override def beforeAll() {
    super.beforeAll()
    campaignsOutData = JsonUtils.readFromJson("campaign/manager", "campaign_output", Schema.campaignOutput)
  }

  "isSchemaEqual" should "return false" in {
    SchemaUtils.isSchemaEqual(Schema.customer, Schema.nls) should be (false)
  }

  "changeSchema" should "add columns" in{
    val res = SchemaUtils.changeSchema(campaignsOutData,Schema.campaignSchema)
    res.printSchema()
    res.collect().foreach(println)
    assert(res.columns.length==7)

  }

}
