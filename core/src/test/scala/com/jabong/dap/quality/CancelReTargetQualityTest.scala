package com.jabong.dap.quality

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets._
import com.jabong.dap.model.ad4push.schema.Ad4pushSchema._
import com.jabong.dap.quality.campaign.CancelReTargetQuality
import org.scalatest.FlatSpec

/**
 * Created by Kapil.Rajak on 12/8/15.
 */
class CancelReTargetQualityTest extends FlatSpec with SharedSparkContext {
  "getSample" should "behave in expected way" in {
    val df = JsonUtils.readFromJson(AD4PUSH, "reduceIn", dfFromCsv)
    val sample = CancelReTargetQuality.getSample(df, .5)
    assert(Math.abs((df.count() / 2) - sample.count()) <= 1)

    val sample1By4 = CancelReTargetQuality.getSample(df, .25)
    assert(Math.abs((df.count() / 4) - sample1By4.count()) <= 1)

    assert(CancelReTargetQuality.getSample(df, 1).collect().toSet.equals(df.collect().toSet))
  }
}
