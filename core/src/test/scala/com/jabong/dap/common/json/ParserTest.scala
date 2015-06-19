package com.jabong.dap.common.json

import net.liftweb.json.JsonParser.ParseException
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by Rachit on 19/6/15.
 */
class ParserTest extends FlatSpec with Matchers {
  case class TestSchema(id: String, name: String) extends EmptyClass

  "Parser" should "successfully parse a valid JSON file" in {
    Parser.parseJson[TestSchema]("test/parserTest1.json")
  }

  "Parser" should "throw ParserException for an invalid JSON file" in {
    a [ParseException] should be thrownBy {
      Parser.parseJson[TestSchema]("test/parserTest2.json")
    }
  }
}
