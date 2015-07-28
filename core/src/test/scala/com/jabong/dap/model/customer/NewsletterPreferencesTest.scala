package com.jabong.dap.model.customer

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.schema.NewsletterPrefSchema
import com.jabong.dap.model.customer.variables.NewsletterPreferences
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by Kapil.Rajak on 9/7/15.
 */
class NewsletterPreferencesTest extends FlatSpec with SharedSparkContext {

  @transient var newsletterSubscription: DataFrame = _
  @transient var newsletterPreferences: DataFrame = _
  @transient var expectedNewsletterPreferences: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    newsletterSubscription = JsonUtils.readFromJson(DataSets.NEWSLETTER_PREFERENCES, DataSets.NEWSLETTER_SUBSCRIPTION, Schema.nls)
    expectedNewsletterPreferences = JsonUtils.readFromJson(DataSets.NEWSLETTER_PREFERENCES, DataSets.NEWSLETTER_PREFERENCES, NewsletterPrefSchema.newsletterPreferences)
  }
  "getIncrementalNewsletterPreferences: Data Frame" should "match with expected output" in {
    //      val parquetFilePathLocal = "/home/jabong/work/parquetFile/newsletter_subscription"
    //      val parquetData = Spark.getSqlContext().read.parquet(parquetFilePathLocal)
    // //     parquetData.limit(50).write.json(DataSets.TEST_RESOURCES + DataSets.NEWSLETTER_PREFERENCES + ".json")
    ////      assert(dfPaybackCustomerResult.collect().toSet.equals(expectedPaybackCustomers.collect().toSet));
    newsletterPreferences = NewsletterPreferences.getIncrementalNewsletterPref(newsletterSubscription)
    //newsletterPreferences.limit(50).write.json(DataSets.TEST_RESOURCES + DataSets.NEWSLETTER_PREFERENCES + ".json")
    newsletterPreferences.collect().toSet.equals(expectedNewsletterPreferences.collect().toSet)
  }
  "getMergedNewsletterPref: Data Frame" should "match with expected output" in {
    val df = JsonUtils.readFromJson(DataSets.NEWSLETTER_PREFERENCES, DataSets.NEWSLETTER_PREFERENCES, NewsletterPrefSchema.newsletterPreferences)
    val updatedDF = JsonUtils.readFromJson(DataSets.NEWSLETTER_PREFERENCES, "newsletter_pref_updated", NewsletterPrefSchema.newsletterPreferences)
    val expectedResult = JsonUtils.readFromJson(DataSets.NEWSLETTER_PREFERENCES, "expected_merged_df", NewsletterPrefSchema.newsletterPreferences)
    val merged = NewsletterPreferences.getMergedNewsletterPref(updatedDF, df)
    assert(merged.collect().toSet.equals(expectedResult.collect().toSet))
  }
}
