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
class ACartCampaign(sQLContext: SQLContext) extends Campaign with java.io.Serializable {

  //Logic to select the cutomer
  // In this case cutomers with abundant cart in last 30days
  def customerSelection(customerVariableData:DataFrame):DataFrame={
    if(customerVariableData==null ){
      return null
    }
    val ACartCustomers = customerVariableData.filter(ACartVariables.ACartStatus+"=1")
    return ACartCustomers
  }


  def selectColumns(customerData:DataFrame,columns: Array[String]): DataFrame ={
    customerData.select(columns(0))

  }



  def customerSkuFilter(customerData:DataFrame):DataFrame={
    if(customerData==null){
      return null
    }

    val filteredData = customerData.withColumn(ACartVariables.ACartSku1,skuFilter(customerData(ACartVariables.ACartPriceSku2)
      ,customerData(ACartVariables.ACartPriceSku2Price)
      ,customerData(ACartVariables.ACartPriceSku2TodayPrice))).withColumn(ACartVariables.ACartSku2,
        skuFilter(customerData(ACartVariables.ACartPriceSku2)
          ,customerData(ACartVariables.ACartPriceSku2Price)
          ,customerData(ACartVariables.ACartPriceSku2TodayPrice))).select(ACartVariables.UID,ACartVariables.ACartSku1,ACartVariables.ACartPriceSku1Brand,ACartVariables.ACartPriceSku1Brick
        ,ACartVariables.ACartPriceSku1Category,ACartVariables.ACartPriceSku1Color,
        ACartVariables.ACartPriceSku1Gender,ACartVariables.ACartSku2,ACartVariables.ACartPriceSku2Brand
        ,ACartVariables.ACartPriceSku2Brick,ACartVariables.ACartPriceSku2Category,
        ACartVariables.ACartPriceSku2Color,ACartVariables.ACartPriceSku2Gender)
      .filter(ACartVariables.ACartSku1+" is not null and "+ACartVariables.ACartSku2+" is not null")

    return filteredData

  }

  def skuPriceFilter(sku:Any,Price:Any,TodayPrice:Any): String ={
    if(sku == null || Price==null || TodayPrice==null){
      return null
    }

    if(TodayPrice.asInstanceOf[Double] < Price.asInstanceOf[Double]){
      return sku.toString
    }
    return null
  }

  //UDFs
  val skuFilter = udf((sku:Any,Price:Any,TodayPrice:Any)=>skuPriceFilter(sku:Any,Price:Any,TodayPrice:Any))
    val skuOrdered = udf((sku:String,skuOrderedList:List[String]) => SkuOrdered(sku:String,skuOrderedList:List[String]))

  def groupCustomerData(orderData:DataFrame): DataFrame = {

    import sQLContext.implicits._

    if (orderData == null) {
      return null
    }
    orderData.foreach(println )
    orderData.printSchema()
    val customerData = orderData.filter(CustomerVariables.CustomerForeignKey+" is not null and sku is not null").select(CustomerVariables.CustomerForeignKey,"sku")

    val customerSkuMap = customerData.map(t=>(t(0),t(1).toString))
    println(customerSkuMap.count())
    val customerGroup = customerSkuMap.groupByKey().map{case (key,value) => (key.toString,value.toList)}

    // .agg($"sku",$+CustomerVariables.CustomerForeignKey)
    val grouped =   customerGroup.toDF(CustomerVariables.CustomerForeignKey,"sku_list")

    return grouped
  }


  def customerOrderFilter(skuFilteredData:DataFrame,orderData:DataFrame):DataFrame={
    if(orderData==null || skuFilteredData==null){
      return null
    }
    orderData.printSchema()
    val customerFilteredData = skuFilteredData.join(orderData
      ,skuFilteredData.col(ACartVariables.UID).equalTo(orderData.col(CustomerVariables.CustomerForeignKey)),"left")
      .withColumn("sku1",skuOrdered(skuFilteredData(ACartVariables.ACartSku1),orderData("sku_list")))
      .withColumn("sku2",skuOrdered(skuFilteredData(ACartVariables.ACartSku2),orderData("sku_list")))
      .select(ACartVariables.UID,"sku1","sku2")

    customerFilteredData.collect().foreach(println)
    return customerFilteredData
  }

  /*To check whether sku has been ordered before or not */
  def SkuOrdered(sku:String,skuOrderedList:List[String]): String ={
    if(sku==null)
      return null


    if(skuOrderedList==null || skuOrderedList.length<1)
      return sku

    for(skuInList <-skuOrderedList){
      if(sku.equals(skuInList)) {

        return null
      }
    }

    return sku
  }


}
