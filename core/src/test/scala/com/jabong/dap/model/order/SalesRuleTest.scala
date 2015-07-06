package com.jabong.dap.model.order

import com.jabong.dap.common.{SharedSparkContext, Spark}
import com.jabong.dap.model.order.variables.SalesRule
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FlatSpec


  /**
  * Created by jabong on 29/6/15.
  */
class SalesRuleTest extends FlatSpec with SharedSparkContext{

   @transient var sqlContext: SQLContext = _
   @transient var df1: DataFrame = _
   @transient var df2: DataFrame = _

   override def beforeAll() {
     super.beforeAll()
     sqlContext = new SQLContext(Spark.getContext())

     df1 = sqlContext.read.json("test/sales_rule1.json")
     df1.collect.foreach(println)
   }

   "The result Dataframe" should "have size 4" in {
     var wcCodes = SalesRule.getCode(df1,1)
     wcCodes.collect.foreach(println)
     assert(wcCodes.collect.size == 4)
   }

   "The result Dataframe" should "have size 3" in {
     var wcCodes = SalesRule.getCode(df1,2)
     wcCodes.collect.foreach(println)
     assert(wcCodes.collect.size == 3)
   }

 }
