package com.jabong.dap.quality.Clickstream


import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf

//import _root_.jabong.ScalaMail
import com.jabong.dap.common.Spark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


/**
 * Created by tejas on 13/8/15.
 */

object DataQualityCheck extends java.io.Serializable {

  def main(args: Array[String]): Unit = {

    Spark.getContext()
    val conf = new SparkConf().setAppName("Clickstream Surf Variables").set("spark.driver.allowMultipleContexts", "true")
    Spark.init(conf)

    val hiveContext = Spark.getHiveContext()
    //val sqlContext = Spark.getSqlContext()
    var gap = args(0).toInt
    var tablename = args(1)
    var tablename1 = args(2)
    var tablename2= args(3)
    var tablename3 = args(4)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -gap)
    val mFormat = new SimpleDateFormat("MM")
    var year = cal.get(Calendar.YEAR).toString
    var day = cal.get(Calendar.DAY_OF_MONTH).toString
    var month = mFormat.format(cal.getTime())
    val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    var dt = dateFormat.format(cal.getTime())
    //var res1 = DataQualityMethods.Artemisdaily(hiveContext, day, month, year, tablename)

    val output = DataQualityMethods.Artemisdaily(hiveContext, day, month, year, tablename, tablename1, tablename2,tablename3)
    write("hdfs://172.16.84.37:8020", "./Automation1/",output.getBytes())
    //write(ConfigConstants.OUTPUT_PATH, "./Automation1/",output.getBytes())
    //ScalaMail.sendMessage("tejas.jain@jabong.com","","","tejas.jain@jabong.com",output,"Quality Report","")

    //println("ScalaMailMain")
  }

  def write(uri: String, filePath: String,data: Array[Byte]) = {
    System.setProperty("HADOOP_USER_NAME", "tjain")
    val path = new Path(filePath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(data)
    fs.close()
  }

}
