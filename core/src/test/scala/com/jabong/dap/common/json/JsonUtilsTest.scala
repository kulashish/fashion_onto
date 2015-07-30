package com.jabong.dap.common.json

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.data.storage.DataSets
import org.scalatest.FlatSpec

/**
 * Created by Kapil.Rajak on 30/7/15.
 */
class JsonUtilsTest extends FlatSpec with SharedSparkContext {

  "jsonsFile2ArrayOfMap: List of Maps" should "be correct with length and content" in {
      val listMaps = JsonUtils.jsonsFile2ArrayOfMap("common","parseToListOfMaps")
      assert(listMaps.length.equals(11))
      assert(listMaps(4)("reaction").equals(9.toString))
      assert(listMaps(4)("device_id").equals("000032E7-9578-42AA-B26C-0BF773A79368"))
  }
}
