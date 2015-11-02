package com.jabong.dap.init

import com.jabong.dap.common.{ Credentials, Config }
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Unit Test class for Config JSON validator.
 */

class ConfigJsonValidatorTest extends FlatSpec with Matchers {

  val outputPath = Option.apply("outputPath")

  // Unit tests for required values.

  "Config Json Validator" should "throw IllegalArgumentException for null application name" in {
    val config = new Config(applicationName = null, readOutputPath = null, writeOutputPath = null, basePath = null, env = null, credentials = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateRequiredValues(config)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty application name" in {
    val config = new Config(applicationName = "", readOutputPath = null, writeOutputPath = null, basePath = null, env = null, credentials = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateRequiredValues(config)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for null outputPath" in {
    val config = new Config(applicationName = "Alchemy Test", readOutputPath = null, writeOutputPath = null, basePath = null, env = null, credentials = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateRequiredValues(config)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty outputPath" in {
    val config = new Config(applicationName = "Alchemy Test", readOutputPath = null, writeOutputPath = null, basePath = "", env = null, credentials = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateRequiredValues(config)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for null base path" in {
    val config = new Config(applicationName = "Alchemy Test", readOutputPath = null, writeOutputPath = outputPath, basePath = null, env = null, credentials = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateRequiredValues(config)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty base path" in {
    val config = new Config(applicationName = "Alchemy Test", readOutputPath = null, writeOutputPath = outputPath, basePath = "", env = null, credentials = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateRequiredValues(config)
    }
  }

  // Unit tests for credentials
  "Config Json Validator" should "throw IllegalArgumentException for null source" in {
    val credentials = new Credentials(source = null, driver = null, server = null, port = null, dbName = null,
      userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty source" in {
    val credentials = new Credentials(source = "", driver = null, server = null, port = null, dbName = null,
      userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for null driver" in {
    val credentials = new Credentials(source = "bob", driver = null, server = null, port = null, dbName = null,
      userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty driver" in {
    val credentials = new Credentials(source = "bob", driver = "", server = null, port = null, dbName = null,
      userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for null server" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = null, port = null, dbName = null,
      userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty server" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "", port = null, dbName = null,
      userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for null port" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "127.0.0.1", port = null,
      dbName = null, userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty port" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "127.0.0.1", port = "",
      dbName = null, userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for null dbName" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "127.0.0.1", port = "3306",
      dbName = null, userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty dbName" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "127.0.0.1", port = "3306",
      dbName = "", userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for null username" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "127.0.0.1", port = "3306",
      dbName = "test", userName = null, password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty username" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "127.0.0.1", port = "3306",
      dbName = "test", userName = "", password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for null password" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "127.0.0.1", port = "3306",
      dbName = "test", userName = "user", password = null)
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for empty password" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "127.0.0.1", port = "3306",
      dbName = "test", userName = "user", password = "")
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for unknown source" in {
    val credentials = new Credentials(source = "test", driver = "mysql", server = "127.0.0.1", port = "3306",
      dbName = "test", userName = "user", password = "password")
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  "Config Json Validator" should "throw IllegalArgumentException for unknown driver" in {
    val credentials = new Credentials(source = "bob", driver = "test", server = "127.0.0.1", port = "3306",
      dbName = "test", userName = "user", password = "password")
    a[IllegalArgumentException] should be thrownBy {
      ConfigJsonValidator.validateCredentials(credentials)
    }
  }

  // Integration test
  "Config Json Validator" should "not throw any exception if everything is validated (with credentials)" in {
    val credentials = new Credentials(source = "bob", driver = "mysql", server = "127.0.0.1", port = "3306",
      dbName = "test", userName = "user", password = "password")
    val config = new Config(applicationName = "Alchemy Test", readOutputPath = null, writeOutputPath = outputPath, basePath = "/test/", env = null,
      credentials = List(credentials))
    ConfigJsonValidator.validate(config)
  }

  "Config Json Validator" should "not throw any exception if everything is validated (without credentials)" in {
    val config = new Config(applicationName = "Alchemy Test", readOutputPath = null, writeOutputPath = outputPath, basePath = "/test/", env = null, credentials = List())
    ConfigJsonValidator.validate(config)
  }
}
