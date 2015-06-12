package com.jabong.dap.common.json

import net.liftweb.json.{ DefaultFormats, parse }

import scala.io.Source

/**
 * Created by Rachit Gupta on 10/6/15.
 */

/**
 * This is just an empty class used for generalizing the parseJSON method.
 * If this method needs to be used, make sure to extend the schema classes
 * from this class.
 */
class EmptyClass

object Parser {
  def parseJson[T <: EmptyClass: Manifest](filePath: String): T = {
    implicit val formats = DefaultFormats
    val source = Source.fromFile(filePath)
    val lines = try source.getLines.mkString("\n") finally source.close()
    val json = parse(lines)
    json.extract[T]
  }
}
