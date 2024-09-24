package com.adevinta.bq.bqwriter.mocks

import com.adevinta.bq.bqwriter.StagingRepository
import com.adevinta.bq.shared.remotedir.AvroGcsFileName
import com.adevinta.bq.shared.remotedir.RemoteFileRef
import zio._
import zio.mock.Mock
import zio.mock.Proxy

object StagingRepositoryMock extends Mock[StagingRepository] {
  object ListDoneAvroFiles extends Effect[Unit, Throwable, Chunk[AvroGcsFileName]]
  object DeleteFiles extends Effect[Chunk[RemoteFileRef], Throwable, Unit]

  val compose: URLayer[Proxy, StagingRepository] = ZLayer {
    ZIO.serviceWith[Proxy] { proxy =>
      new StagingRepository {
        override def listDoneAvroFiles(): Task[Chunk[AvroGcsFileName]] =
          proxy(ListDoneAvroFiles)

        override def deleteFiles(refs: Chunk[RemoteFileRef]): Task[Unit] =
          proxy(DeleteFiles, refs)

        override def runDoneFileCleanup(): ZIO[Any, Throwable, Unit] =
          ???
      }
    }
  }
}
