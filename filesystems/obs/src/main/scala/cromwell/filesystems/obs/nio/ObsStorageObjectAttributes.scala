package cromwell.filesystems.obs.nio

import java.nio.file.attribute.FileTime

import com.obs.services.model.ObjectMetadata

import scala.collection.mutable.Map

final case class ObsStorageObjectAttributes(objectMeta: ObjectMetadata, path: ObsStoragePath) extends ObsStorageFileAttributes {
  override def creationTime(): FileTime = {
    FileTime.fromMillis(objectMeta.getLastModified.getTime)
  }

  override def lastAccessTime(): FileTime = FileTime.fromMillis(0)

  override def lastModifiedTime(): FileTime = creationTime()

  override def isRegularFile: Boolean = true

  override def isDirectory: Boolean = false

  override def isSymbolicLink: Boolean = false

  override def isOther: Boolean = false

  override def size(): Long = objectMeta.getContentLength

  override def fileKey(): AnyRef = path.pathAsString

  // obs sdk has an issule: throw NullPointerException when no expire time exists.
  override def expires: FileTime = FileTime.fromMillis(0)

  override def cacheControl(): Option[String] = None

  override def contentDisposition: Option[String] = None

  override def contentEncoding: Option[String] = Option(objectMeta.getContentEncoding)

  override def etag: Option[String] = Option(objectMeta.getEtag)

  override def userMeta: Map[String, String] = Map.empty[String, String]
}
