package com.adevinta.bq.gcswriter.util

import com.adevinta.bq.gcswriter.fixtures.TestRecord
import com.adevinta.bq.gcswriter.utils.AvroUtil
import zio._
import zio.test._

object AvroUtilSpec extends ZIOSpecDefault {

  private val testRecord = TestRecord()

  override def spec: Spec[TestEnvironment with Scope, Any] = {
    suite("AvroUtilSpec")(
      test("extracts a field from a generic record") {
        for {
          extractedField <- AvroUtil.stringField(testRecord, List("someRecord", "value"))
        } yield {
          assertTrue(extractedField.contains(testRecord.someRecord.value))
        }
      },
      test("fail if a path it is not present in the record") {
        for {
          result <- AvroUtil.stringField(testRecord, List("not", "present")).exit
        } yield {
          assertTrue(result.isFailure)
        }
      },
      test("return None is the specified path leads to a GenericRecord") {
        for {
          extractedField <- AvroUtil.stringField(testRecord, List("someRecord"))
        } yield {
          assertTrue(extractedField.isEmpty)
        }
      },
    )
  }

}
