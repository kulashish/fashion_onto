package com.jabong.data.collection.json

import scala.io.Source

/**
 * Created by Rachit on 9/6/15.
 */
class Parser(master: String) extends java.io.Serializable {
  def Start(): Unit = {
    val confFilePath = "/home/rachit/projects/Alchemy/src/main/scala/com/jabong/conf/tables.json"

    val source = Source.fromFile(confFilePath)
    val lines = try source.getLines mkString "\n" finally source.close()
  }
}
