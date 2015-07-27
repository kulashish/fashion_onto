package com.jabong.dap.model.product.itr

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.time.{TimeUtils, TimeConstants}
import org.apache.commons.net.ntp.TimeStamp
import org.scalatest.FlatSpec
import java.math.BigDecimal

/**
 * Created by rahul for com.jabong.dap.model.product.itr on 25/7/15.
 */
class BasicBobTest extends FlatSpec{


"Zero value for special price " should "return price" in {
  val specialPrice = new BigDecimal(0.0)
  val price = new BigDecimal(10.0)
  val expectedPrice = BasicBob.correctPrice(specialPrice,price,null,null,null)
  assert(expectedPrice == price)
}


  "null value for from date for special price " should "return price" in {
    val specialPrice = new BigDecimal(20.0)
    val price = new BigDecimal(10.0)
    val expectedPrice = BasicBob.correctPrice(specialPrice,price,null,null,null)
    assert(expectedPrice == price)
  }


  "current date falls in between special price from date and to date " should "return special Price" in {
    val specialPrice = new BigDecimal(20.0)
    val price = new BigDecimal(10.0)
    val cal = Calendar.getInstance();

    val sdf = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT_MS)
    val rightNowDate = sdf.format(cal.getTime())
    val yesterdayTimeStamp = TimeUtils.getEndTimestampMS(java.sql.Timestamp.valueOf(rightNowDate))
    cal.add(Calendar.DATE, -2)
    val yesterdayDate = new java.sql.Date(cal.getTime().getTime);
    cal.add(Calendar.DATE, 4)
    val nextDayDate = new java.sql.Date(cal.getTime().getTime);

    val expectedPrice = BasicBob.correctPrice(specialPrice,price,yesterdayDate,nextDayDate,yesterdayTimeStamp)
    assert(expectedPrice == specialPrice)
  }

  "current date doesn't falls in between special price from date and to date " should "return  Price" in {
    val specialPrice = new BigDecimal(20.0)
    val price = new BigDecimal(10.0)
    val cal = Calendar.getInstance();

    val sdf = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT_MS)
    val rightNowDate = sdf.format(cal.getTime())
    val yesterdayTimeStamp = TimeUtils.getEndTimestampMS(java.sql.Timestamp.valueOf(rightNowDate))
    cal.add(Calendar.DATE, 12)
    val yesterdayDate = new java.sql.Date(cal.getTime().getTime);
    cal.add(Calendar.DATE, 4)
    val nextDayDate = new java.sql.Date(cal.getTime().getTime);

    val expectedPrice = BasicBob.correctPrice(specialPrice,price,yesterdayDate,nextDayDate,yesterdayTimeStamp)
    assert(expectedPrice == price)
  }

}
