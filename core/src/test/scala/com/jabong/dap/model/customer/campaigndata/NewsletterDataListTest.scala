package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 27/10/15.
 */
class NewsletterDataListTest extends FlatSpec with SharedSparkContext {

  @transient var dfContactListMobileIncr: DataFrame = _
  @transient var dfContactListMobilePrevFull: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    dfContactListMobileIncr = JsonUtils.readFromJson(DataSets.NL_DATA_LIST, "contact_list_mobile_incr")
    dfContactListMobilePrevFull = JsonUtils.readFromJson(DataSets.NL_DATA_LIST, "contact_list_mobile_full")

  }

  "getNLDataList: Data Frame size" should "2" in {

    val (df) = NewsletterDataList.getNLDataList(dfContactListMobileIncr, dfContactListMobilePrevFull)

    //    dfInc.collect().foreach(println)
    //    dfInc.printSchema()
    //
    //    dfFullFinal.collect().foreach(println)
    //    dfFullFinal.printSchema()

    assert(df.count() == 2)
  }

}