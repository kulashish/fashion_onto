package com.jabong.dap.data.acq.common

import com.jabong.dap.common.json.parser.EmptyClass

/**
 * Created by Rachit on 10/6/15.
 */

// Classes for storing the JSON schema.
case class JoinTables(name: String, foreignKey: String)

case class TableInfo(source: String, tableName: String, primaryKey: String, mode: String, saveFormat: String,
                     dateColumn: String, rangeStart: String, rangeEnd: String, limit: String,
                     joinTables: List[JoinTables])

case class ImportInfo(acquisition: List[TableInfo]) extends EmptyClass
