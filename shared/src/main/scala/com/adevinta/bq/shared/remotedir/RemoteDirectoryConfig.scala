package com.adevinta.bq.shared.remotedir

/** Configuration for a flat remote file system.
  *
  * @param baseUri
  *   directory where the files will be stored, must be an URI in one of the following formats:
  *   `gs://bucket-name/path/to/dir`, `file:///path/to/dir`
  */
final case class RemoteDirectoryConfig(
    baseUri: String,
)
