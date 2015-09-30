package com.jabong.dap.data.acq.common

import com.jabong.dap.common.json.EmptyClass

/**
 * Case class for storing information about join tables.
 *
 * @param name String The name of the table.
 * @param foreignKey String The name of the foreign key for the table.
 * @param selectString String The select string for selecting columns from the join table.
 */
case class JoinTables(
  name: String,
  foreignKey: String,
  selectString: Option[String])

/**
 * Case class for storing information about data acquisition from a table.
 *
 * @param source String The source of the data. (Can be erp, bob, unicommerce or nextbee)
 * @param tableName String The name of the table.
 * @param primaryKey String The primary key of the table.
 * @param mode String The mode of the data acquisition. (Can be full, daily or hourly)
 * @param saveFormat String The format in which the data is to be saved. (Can be orc or parquet)
 * @param saveMode String The mode in which the data is to be saved. (Can be overwrite, append, error or ignore)
 * @param dateColumn String The name of the column which represents the date time when the row was updated.
 * @param rangeStart String The date time from which the data is to be fetched.
 * @param rangeEnd String The date time till which the data is to be fetched.
 * @param limit String The number of rows to be fetched.
 * @param filterCondition String Condition to filter the primary key while fetching data.
 * @param joinTables List[JoinTables] List of tables to be joined.
 */
case class TableInfo(
  source: String,
  tableName: String,
  primaryKey: String,
  mode: String,
  saveFormat: String,
  saveMode: String,
  dateColumn: Option[String],
  rangeStart: Option[String],
  rangeEnd: Option[String],
  limit: Option[String],
  filterCondition: Option[String],
  joinTables: Option[List[JoinTables]])

/**
 * Case class for storing information for merging the data of a table.
 *
 * @param source String The source of the data. (Can be erp, bob, unicommerce or nextbee)
 * @param tableName String The name of the table.
 * @param primaryKey String The primary key of the table.
 * @param mergeMode String The mode of the data merge.
 * @param dateColumn String The name of the column which represents the date time when the row was updated.
 * @param incrDate String The date for the merge data is to be run.
 * @param fullDate String The full data date. Incase of merging incr data with full data from a specific.
 * @param incrMode String The incremental data is in monthly or daily mode
 * @param saveMode String The mode in which the data is to be saved. (Can be overwrite, append, error or ignore)
 */

case class MergeInfo(
  source: String,
  tableName: String,
  primaryKey: String,
  mergeMode: String,
  dateColumn: Option[String],
  incrDate: Option[String],
  fullDate: Option[String],
  incrMode: Option[String],
  saveMode: String)

/**
 * Case class for storing information for variable merging the data of customer and order.
 *
 * @param source String source for which to be run
 * @param incrDate String The date for the merge data is to be run.
 * @param incrMode String data mode
 * @param saveFormat String The Format in which the data will be found and saved after the merge.
 * @param saveMode String The mode in which the data is to be saved. (Can be overwrite, append, error or ignore)
 */

case class ParamInfo(
  source: String,
  input: Option[String],
  fullDate: Option[String],
  incrDate: Option[String],
  path: Option[String],
  incrMode: Option[String],
  saveFormat: String,
  saveMode: String,
  fraction: Option[String])

/**
 * Case class for storing the information for the data acquisition.
 *
 * @param acquisition List[TableInfo] List of tables to acquire the data from.
 */
case class ImportInfo(
  isHistory: Option[Boolean],
  acquisition: List[TableInfo]) extends EmptyClass

/**
 * Case class for storing the information for the merge job.
 *
 * @param merge List[MergeInfo] List of Tables to run the merge job on.
 */
case class MergeJobInfo(
  isHistory: Option[Boolean],
  merge: List[MergeInfo]) extends EmptyClass

/**
 * Case class for storing the information of parameters to execute different jobs.
 *
 * @param params List[ParamInfo] List of parameters to execute different jobs.
 */
case class ParamJobInfo(
  isHistory: Option[Boolean],
  params: List[ParamInfo]) extends EmptyClass

/**
 * Object to access job params application wide
 */
object ParamJobConfig {
  var paramJobInfo: ParamJobInfo = null
  var paramInfo: ParamInfo = null
}

/**
 * Object to access config variables application wide
 */
object MergeJobConfig {
  var mergeJobInfo: MergeJobInfo = null
  var mergeInfo: MergeInfo = null
}

/**
 * Object to access ImportInfo variables application wide
 */
object AcqImportInfo {
  var importInfo: ImportInfo = null
  var tableInfo: TableInfo = null

}

//FIXME:need to check which one to use
//case class for campaign List
case class CampaignDetail(
  campaignName: String,
  priority: Int,
  mailType: Int)

//case class CampaignList (
//pushCampaignList: List[CampaignDetail]) extends EmptyClass

/**
 *
 * @param pushBasePath
 * @param pushCampaignList
 */
//case class for campaignConfig expects path and campaign List
case class CampaignConfig(
  var pushBasePath: String,
  var pushCampaignList: List[CampaignDetail],
  var emailSubscribers: Option[String])

object CampaignInfo {
  var campaignJobInfo: String = null
  var campaigns: CampaignConfig = null
}

