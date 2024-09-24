package com.adevinta.bq.shared.remotedir

import zio._
import zio.test.{suite => zioSuite, test => zioTest, _}

import java.nio.charset.StandardCharsets.UTF_8
import java.util.HexFormat

object RemoteDirectorySuite {
  private val randomBytes = HexFormat.of().parseHex(
    "689311d22a7c452b8a1540504c40d0cff786bb8cc20f0c924fa79863a1d26f94d0795421aa3d5acd75de64923c0415d0ad1f" +
      "2459f525ff7b179428cadd333606f9301298a2f0e5a8785a7b7a84f35d625fc4e9c8b59f228dbd1c0e79291740f04a1daba3" +
      "44449d46b3b62377b638e4c203e8ecee7b363c94238d7a84ee589630173de69e3f6bc220846b6be94da4a31065a0ae2b8588" +
      "706e73dec00e7f247f5d1b46281fce5abbdc1326d81b82b5c41e0b0ce10f57cbdf4eac08b03fb1e7dfd9f4f728814b1f4178"
  )

  def remoteDirectorySuite(name: String): Spec[Scope & RemoteDirectory, Throwable] =
    zioSuite(name)(
      zioTest("make file and list") {
        for {
          remoteDirectory <- ZIO.service[RemoteDirectory]
          _ <- remoteDirectory.makeFile("test1", "test1".getBytes(UTF_8))
          _ <- remoteDirectory.makeFile("test2", "test2".getBytes(UTF_8))
          _ <- remoteDirectory.makeFile("sub/test3", "test3".getBytes(UTF_8))
          list <- remoteDirectory.list.runCollect
        } yield {
          assertTrue(
            list.contains("test1"),
            list.contains("test2"),
            list.contains("sub/test3"),
            !list.contains("sub")
          )
        }
      },
      zioTest("list full file refs") {
        for {
          remoteDirectory <- ZIO.service[RemoteDirectory]
          _ <- remoteDirectory.makeFile("test1", "test1".getBytes(UTF_8))
          _ <- remoteDirectory.makeFile("test2", "test2".getBytes(UTF_8))
          _ <- remoteDirectory.makeFile("sub/test3", "test3".getBytes(UTF_8))
          list <- remoteDirectory.listFiles.runCollect
        } yield {
          assertTrue(
            list.forall { fileRef =>
              (fileRef.schema == "gs" || fileRef.schema == "file") &&
              (fileRef.host == "" || !fileRef.host.contains("/")) &&
              (!fileRef.prefix.startsWith("/") && !fileRef.prefix.endsWith("/")) &&
              (!fileRef.fileName.startsWith("/"))
            }
          )
        }
      },
      zioTest("make file with contents") {
        for {
          remoteDirectory <- ZIO.service[RemoteDirectory]
          _ <- remoteDirectory.makeFile("content1", randomBytes)
          readContents <- remoteDirectory.readFile("content1")
        } yield {
          assertTrue(
            readContents sameElements randomBytes
          )
        }
      },
      zioTest("make file with contents in sub-directory") {
        for {
          remoteDirectory <- ZIO.service[RemoteDirectory]
          _ <- remoteDirectory.makeFile("subdir/content1", randomBytes)
          _ <- remoteDirectory.makeFile("subdir/content2", randomBytes)
          readContents1 <- remoteDirectory.readFile("subdir/content1")
          readContents2 <- remoteDirectory.readFile("subdir/content2")
        } yield {
          assertTrue(
            readContents1 sameElements randomBytes,
            readContents2 sameElements randomBytes
          )
        }
      },
      zioTest("make file with streaming content") {
        for {
          remoteDirectory <- ZIO.service[RemoteDirectory]
          out <- remoteDirectory.makeFileFromStream("content2")
          _ <- ZIO.attemptBlocking {
            out.write(randomBytes)
            out.write(randomBytes)
            out.close()
          }
          readContents <- remoteDirectory.readFile("content2")
        } yield {
          assertTrue(
            readContents sameElements (randomBytes ++ randomBytes)
          )
        }
      },
      zioTest("fails to read missing file") {
        for {
          remoteDirectory <- ZIO.service[RemoteDirectory]
          readResult <- remoteDirectory.readFile("non-existing-file").exit
        } yield {
          assertTrue(
            readResult.isFailure,
            readResult.causeOption.flatMap(_.failureOption).contains(
              RemoteFileNotFound("non-existing-file")
            )
          )
        }
      },
      zioTest("deletes files") {
        for {
          remoteDirectory <- ZIO.service[RemoteDirectory]
          fileName1 = "test" + java.util.UUID.randomUUID().toString
          fileName2 = "sub-dir/test" + java.util.UUID.randomUUID().toString
          _ <- remoteDirectory.makeFile(fileName1, fileName1.getBytes(UTF_8))
          _ <- remoteDirectory.makeFile(fileName2, fileName2.getBytes(UTF_8))
          remoteRefsBeforeDelete <- remoteDirectory.listFiles.runCollect
          _ <- remoteDirectory.deleteFiles(Chunk(fileName1, fileName2))
          filesAfterDelete <- remoteDirectory.list.runCollect
        } yield {
          assertTrue(
            remoteRefsBeforeDelete.size >= 2,
            !filesAfterDelete.contains(fileName1),
            !filesAfterDelete.contains(fileName2),
          )
        }
      }
    )

}
