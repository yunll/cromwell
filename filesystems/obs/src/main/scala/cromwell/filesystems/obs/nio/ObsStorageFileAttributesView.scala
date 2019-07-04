package cromwell.filesystems.obs.nio

import java.nio.file.NoSuchFileException
import java.nio.file.attribute.{BasicFileAttributeView, FileTime}

import com.obs.services.ObsClient
import com.obs.services.model.GetObjectMetadataRequest

import scala.util.Try

final case class ObsStorageFileAttributesView(obsClient: ObsClient, path: ObsStoragePath) extends BasicFileAttributeView {
  override def name(): String = ObsStorageFileSystem.URI_SCHEMA

  override def readAttributes(): ObsStorageFileAttributes = {
    val obsPath = ObsStoragePath.checkPath(path)

    if (obsPath.seemsLikeDirectory) {
      return ObsStorageDirectoryAttributes(path)
    }

    val request = new GetObjectMetadataRequest(obsPath.bucket, obsPath.key)
    if (Option(obsClient.getObjectMetadata(request)).isEmpty) {
      throw new NoSuchFileException(path.toString)
    }

    val objectMeta = ObsStorageRetry.fromTry(
      () => Try{
        obsClient.getObjectMetadata(path.bucket, path.key)
      }
    )

    ObsStorageObjectAttributes(objectMeta, path)
  }

  override def setTimes(lastModifiedTime: FileTime, lastAccessTime: FileTime, createTime: FileTime): Unit = throw new UnsupportedOperationException("OBS object is immutable")
}
