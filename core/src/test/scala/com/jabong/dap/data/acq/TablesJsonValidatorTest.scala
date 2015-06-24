package com.jabong.dap.data.acq

import java.text.ParseException

import com.jabong.dap.data.acq.common.{ ImportInfo, TableInfo }
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Unit Test class for Tables JSON validator.
 */

class TablesJsonValidatorTest extends FlatSpec with Matchers {

  // Unit tests for required values.
  "Tables Json Validator" should "throw IllegalArgumentException for null source" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = null, saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null, filterCondition = null,
      joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for empty source" in {
    val tableInfo = new TableInfo(source = "", tableName = null, primaryKey = null, mode = null, saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null, filterCondition = null,
      joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for null table name" in {
    val tableInfo = new TableInfo(source = "bob", tableName = null, primaryKey = null, mode = null, saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null, filterCondition = null,
      joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for empty table name" in {
    val tableInfo = new TableInfo(source = "bob", tableName = "", primaryKey = null, mode = null, saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null, filterCondition = null,
      joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for null mode" in {
    val tableInfo = new TableInfo(source = "bob", tableName = "catalog_config", primaryKey = null, mode = null,
      saveFormat = null, saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null,
      filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for empty mode" in {
    val tableInfo = new TableInfo(source = "bob", tableName = "catalog_config", primaryKey = null, mode = "",
      saveFormat = null, saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null,
      filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for null save mode" in {
    val tableInfo = new TableInfo(source = "bob", tableName = "catalog_config", primaryKey = null, mode = "full",
      saveFormat = null, saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null,
      filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for empty save mode" in {
    val tableInfo = new TableInfo(source = "bob", tableName = "catalog_config", primaryKey = null, mode = "full",
      saveFormat = null, saveMode = "", dateColumn = null, rangeStart = null, rangeEnd = null, limit = null,
      filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for null save format" in {
    val tableInfo = new TableInfo(source = "bob", tableName = "catalog_config", primaryKey = null, mode = "full",
      saveFormat = null, saveMode = "overwrite", dateColumn = null, rangeStart = null, rangeEnd = null, limit = null,
      filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for empty save format" in {
    val tableInfo = new TableInfo(source = "bob", tableName = "catalog_config", primaryKey = null, mode = "full",
      saveFormat = "", saveMode = "overwrite", dateColumn = null, rangeStart = null, rangeEnd = null, limit = null,
      filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRequiredValues(tableInfo)
    }
  }

  // Unit tests for possible values.

  "Tables Json Validator" should "throw IllegalArgumentException for unknown source" in {
    val tableInfo = new TableInfo(source = "abc", tableName = null, primaryKey = null, mode = null, saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null, filterCondition = null,
      joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validatePossibleValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for unknown mode" in {
    val tableInfo = new TableInfo(source = "bob", tableName = null, primaryKey = null, mode = "abc", saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null, filterCondition = null,
      joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validatePossibleValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for unknown save format" in {
    val tableInfo = new TableInfo(source = "bob", tableName = null, primaryKey = null, mode = "full", saveFormat = "abc",
      saveMode = "abc", dateColumn = null, rangeStart = null, rangeEnd = null, limit = null, filterCondition = null,
      joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validatePossibleValues(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException for unknown save mode" in {
    val tableInfo = new TableInfo(source = "bob", tableName = null, primaryKey = null, mode = "full", saveFormat = "orc",
      saveMode = "abc", dateColumn = null, rangeStart = null, rangeEnd = null, limit = null, filterCondition = null,
      joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validatePossibleValues(tableInfo)
    }
  }

  // Unit tests for date time validations.

  "Tables Json Validator" should "throw IllegalArgumentException if only one of rangeStart and rangeEnd has value" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = null, saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = "2015-06-22 15:00:00", rangeEnd = null, limit = null,
      filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateDateTimes(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException if range start is not provided for hourly mode" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "hourly", saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = null, rangeEnd = null, limit = null,
      filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateDateTimes(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException if range end is not provided for hourly mode" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "hourly", saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = "2015-06-22 15:00:00", rangeEnd = null, limit = null,
      filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateDateTimes(tableInfo)
    }
  }

  "Tables Json Validator" should "throw ParseException if format of rangeStart is not proper" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = null, saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = "2015-06", rangeEnd = null,
      limit = null, filterCondition = null, joinTables = null)
    a[ParseException] should be thrownBy {
      TablesJsonValidator.validateRanges(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException if rangeStart is greater than rangeEnd" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = null, saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = "2015-06-22 15:00:00", rangeEnd = "2015-05-21 15:00:00",
      limit = null, filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRanges(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException if the range spans more than a month in daily mode" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "daily", saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = "2015-04-22 15:00:00", rangeEnd = "2015-06-21 15:00:00",
      limit = null, filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRanges(tableInfo)
    }
  }

  "Tables Json Validator" should "throw IllegalArgumentException if the range spans more than a day in hourly mode" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "hourly", saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = "2015-04-22 15:00:00", rangeEnd = "2015-04-23 15:00:00",
      limit = null, filterCondition = null, joinTables = null)
    a[IllegalArgumentException] should be thrownBy {
      TablesJsonValidator.validateRanges(tableInfo)
    }
  }

  "Tables Json Validator" should "not throw any exception if the ranges are provided with full mode" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "full", saveFormat = null,
      saveMode = null, dateColumn = null, rangeStart = "2015-04-22 15:00:00", rangeEnd = "2015-04-23 15:00:00",
      limit = null, filterCondition = null, joinTables = null)
    TablesJsonValidator.validateRanges(tableInfo)
  }

  // Integration test.
  "Tables Json Validator" should "not throw any exception if everything is correct (daily mode)" in {
    val tableInfo = new TableInfo(source = "bob", tableName = "catalog_config", primaryKey = "id_catalog_config",
      mode = "daily", saveFormat = "orc", saveMode = "overwrite", dateColumn = "created_at",
      rangeStart = "2015-06-20 15:00:00", rangeEnd = "2015-06-30 15:00:00", limit = null, filterCondition = null,
      joinTables = null)
    val importInfo = new ImportInfo(acquisition = List(tableInfo))
    TablesJsonValidator.validate(importInfo)
  }

  "Tables Json Validator" should "not throw any exception if everything is correct (full mode)" in {
    val tableInfo = new TableInfo(source = "bob", tableName = "catalog_config", primaryKey = "id_catalog_config",
      mode = "full", saveFormat = "orc", saveMode = "overwrite", dateColumn = "created_at", rangeStart = null,
      rangeEnd = null, limit = "3000", filterCondition = null, joinTables = null)
    val importInfo = new ImportInfo(acquisition = List(tableInfo))
    TablesJsonValidator.validate(importInfo)
  }
}
