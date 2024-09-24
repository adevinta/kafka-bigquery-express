package com.adevinta.bq.gcswriter.mocks

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import zio._
import zio.mock.Mock
import zio.mock.Proxy

import java.util
import java.util.Optional

//noinspection NotImplementedCode
object MockSchemaRegistryClient extends Mock[SchemaRegistryClient] {
  object GetSchemaById extends Method[Int, Throwable, ParsedSchema]

  val compose: ZLayer[Proxy, Nothing, SchemaRegistryClient] = ZLayer {
    ZIO.serviceWithZIO[Proxy] { proxy =>
      withRuntime[Any, SchemaRegistryClient] { rts =>
        ZIO.succeed {
          new SchemaRegistryClient {
            override def getSchemaById(id: Int): ParsedSchema = {
              Unsafe.unsafe { implicit u =>
                rts.unsafe.run(proxy(GetSchemaById, id)).getOrThrow()
              }
            }

            // Methods below are not mocked.

            override def parseSchema(
                schemaType: String,
                schemaString: String,
                references: util.List[SchemaReference]
            ): Optional[ParsedSchema] = ???

            override def register(subject: String, schema: ParsedSchema, version: Int, id: Int)
                : Int = ???

            override def register(subject: String, schema: ParsedSchema): Int = ???

            override def getSchemaBySubjectAndId(subject: String, id: Int): ParsedSchema = ???

            override def getAllSubjectsById(id: Int): util.Collection[String] = ???

            override def getLatestSchemaMetadata(subject: String): SchemaMetadata = ???

            override def getSchemaMetadata(subject: String, version: Int): SchemaMetadata = ???

            override def getVersion(subject: String, schema: ParsedSchema): Int = ???

            override def getAllVersions(subject: String): util.List[Integer] = ???

            override def testCompatibility(subject: String, schema: ParsedSchema): Boolean = ???

            override def getCompatibility(subject: String): String = ???

            override def setMode(mode: String): String = ???

            override def setMode(mode: String, subject: String): String = ???

            override def getMode: String = ???

            override def getMode(subject: String): String = ???

            override def getAllSubjects: util.Collection[String] = ???

            override def getId(subject: String, schema: ParsedSchema): Int = ???

            override def updateCompatibility(subject: String, compatibility: String): String = ???

            override def reset(): Unit = ???
          }
        }
      }
    }
  }
}
