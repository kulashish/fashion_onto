package oneTimeScripts

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ContactListMobileVars, PageVisitVariables, CustomerVariables}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by mubarak on 19/10/15.
 */
object AddingUidColumn {

  def addUId(cmr: DataFrame, contactList: DataFrame): DataFrame = {

    val res = cmr.join(contactList, cmr(CustomerVariables.EMAIL) === contactList(CustomerVariables.EMAIL), SQL.LEFT_OUTER)
                .select(
                      cmr(CustomerVariables.RESPONSYS_ID),
                      cmr(CustomerVariables.ID_CUSTOMER),
                      cmr(CustomerVariables.EMAIL),
                      cmr(PageVisitVariables.BROWSER_ID),
                      cmr(PageVisitVariables.DOMAIN),
                      contactList(ContactListMobileVars.UID)
      )
    res
  }

  def main(args: Array[String]) {
    val date = args(0).trim

    val fullPath = args(1).trim

    val saveDate = args(2).trim

    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, saveDate)

    val contactList = DataReader.getDataFrame4mCsv(fullPath, "true", ";")

    val cmr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, date)
  
    val uid = addUId(cmr, contactList)

    DataWriter.writeParquet(uid, savePath, DataSets.FULL_MERGE_MODE)

  }


}
