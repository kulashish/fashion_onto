package com.jabong.dap.campaign.common

import javax.jdo.annotations.Columns

import com.jabong.dap.common.constants.variables.{CustomerVariables, ACartVariables}
import com.jabong.dap.common.Spark
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, SQLContext, DataFrame}
import org.apache.spark.sql.functions._


/**
 * Created by jabong1145 on 16/6/15.
 */
class ACartCampaign(sQLContext: SQLContext) extends LiveCampaign with java.io.Serializable {

  //Logic to select the cutomer
  // In this case cutomers with abundant cart in last 30days
  def customerSelection(customerVariableData:DataFrame):DataFrame={
    if(customerVariableData==null ){
      return null
    }
    val ACartCustomers = customerVariableData.filter(ACartVariables.ACART_STATUS+"=1")
    return ACartCustomers
  }


  def selectColumns(customerData:DataFrame,columns: Array[String]): DataFrame ={
    customerData.select(columns(0))

  }



  def groupCustomerData(orderData:DataFrame): DataFrame = {

    import sQLContext.implicits._

    if (orderData == null) {
      return null
    }
    orderData.foreach(println )
    orderData.printSchema()
    val customerData = orderData.filter(CustomerVariables.FK_CUSTOMER+" is not null and sku is not null")
      .select(CustomerVariables.FK_CUSTOMER,"sku")

    val customerSkuMap = customerData.map(t=>(t(0),t(1).toString))
    println(customerSkuMap.count())
    val customerGroup = customerSkuMap.groupByKey().map{case (key,value) => (key.toString,value.toList)}

    // .agg($"sku",$+CustomerVariables.CustomerForeignKey)
    val grouped =   customerGroup.toDF(CustomerVariables.FK_CUSTOMER,"sku_list")

    return grouped
  }






}
