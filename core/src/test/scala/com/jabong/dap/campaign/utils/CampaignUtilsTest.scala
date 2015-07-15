package com.jabong.dap.campaign.utils

import java.text.{ DateFormat, SimpleDateFormat }
import java.util.Calendar

import org.scalatest.FlatSpec

/**
 * Utilities test class
 */
class CampaignUtilsTest extends FlatSpec {
  val calendar = Calendar.getInstance()
  calendar.add(Calendar.DATE, -1)
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
  val testDate = dateFormat.format(calendar.getTime)

  "Yesterdays date " should "return 1 in day diff" in {
    val diff = CampaignUtils.currentTimeDiff(testDate, "days")
    assert(diff == 1)
  }

  "Yesterdays date " should "return number of hours in time diff" in {
    val diff = CampaignUtils.currentTimeDiff(testDate, "hours")
    assert(diff >= 23 && diff <= 24)
  }

  "Yesterdays date " should "return number of minutes in time diff" in {
    val diff = CampaignUtils.currentTimeDiff(testDate, "minutes")
    assert(diff <= 1440)
  }

  //
  //  "Given data format " should "return current time in that format" in {
  //    val currentTime = CampaignUtils.now("yyyy/mm/dd")
  //    assert(currentTime=="2015/07/13")
  //  }

}
