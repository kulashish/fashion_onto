package com.jabong.dap.common.json

import com.jabong.dap.data.acq.common.{ MergeJobConfig, MergeJobInfo }
import com.jabong.dap.data.storage.merge.MergeJsonValidator
import net.liftweb.json.JsonParser.ParseException
import org.scalatest.{ FlatSpec, Matchers }

/**
 * Created by Rachit on 19/6/15.
 */
class ParserTest extends FlatSpec with Matchers {
  case class TestSchema(id: String, name: String) extends EmptyClass

  "Parser" should "successfully parse a valid JSON file" in {
    Parser.parseJson[TestSchema](JsonUtils.TEST_RESOURCES + "/common/json/parserTest1.json")
  }

  "Parser" should "throw ParserException for an invalid JSON file" in {
    a[ParseException] should be thrownBy {
      Parser.parseJson[TestSchema](JsonUtils.TEST_RESOURCES + "/common/json/parserTest2.json")
    }
  }

  "Parser" should "successfully parse a list value" in {
    MergeJobConfig.mergeJobInfo = Parser.parseJson[MergeJobInfo](JsonUtils.TEST_RESOURCES + "/common/json/example.json")
    MergeJsonValidator.validate(MergeJobConfig.mergeJobInfo)

  }
}
