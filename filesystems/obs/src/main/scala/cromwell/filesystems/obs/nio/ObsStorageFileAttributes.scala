package cromwell.filesystems.obs.nio

import java.nio.file.attribute.{BasicFileAttributes, FileTime}

import scala.collection.mutable.Map

trait ObsStorageFileAttributes extends BasicFileAttributes {
  def cacheControl(): Option[String]

  def contentDisposition: Option[String]

  def contentEncoding: Option[String]

  def expires: FileTime

  def etag: Option[String]

  def userMeta: Map[String, String]
}
