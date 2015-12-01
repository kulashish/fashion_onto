package com.jabong.dap.model.customer.data

import java.util.UUID
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, PageVisitVariables, CustomerVariables }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * Created by mubarak on 7/10/15.
 */
object UUIDGenerator {

  var uidsList = scala.collection.mutable.ListBuffer[String]()

  def getUid(uids: ListBuffer[String]): String = {
    var id = ""
    id = UUID.randomUUID().toString().replaceAll("-", "").toUpperCase() // 7f97d378-4c73-4428-90ec-1dac34f7d6a7 -> 145E6C95AF9B4ED393E3AF387FD74972
    while (uids.contains(id)) {
      id = UUID.randomUUID().toString().replaceAll("-", "").toUpperCase()
    }
    id
  }

  val addUids = udf((s: String) => addUid(s: String))

  def addUid(uid: String): String = {
    var newId: String = null
    if (null == uid) {
      newId = getUid(uidsList)
      uidsList += newId
      newId
    } else {
      uidsList += uid
      uid
    }
  }

  def addUid(cmr: DataFrame): DataFrame = {
    val res = cmr.select(
      addUids(cmr(ContactListMobileVars.UID)) as ContactListMobileVars.UID,
      cmr(CustomerVariables.EMAIL),
      cmr(CustomerVariables.RESPONSYS_ID),
      cmr(CustomerVariables.ID_CUSTOMER),
      cmr(PageVisitVariables.BROWSER_ID),
      cmr(PageVisitVariables.DOMAIN)
    )
    res
  }

}
