package cromwell.filesystems.obs.nio

import java.io.{BufferedOutputStream, OutputStream}
import java.net.URI
import java.nio.channels.SeekableByteChannel
import java.nio.file._
import java.nio.file.attribute.{BasicFileAttributeView, BasicFileAttributes, FileAttribute, FileAttributeView}
import java.nio.file.spi.FileSystemProvider
import java.util

import com.google.common.collect.AbstractIterator
import com.obs.services.ObsClient
import com.obs.services.exception.ObsException
import com.obs.services.model.{GetObjectMetadataRequest, ListObjectsRequest}

import scala.collection.JavaConverters._
import scala.collection.immutable.Set
import collection.mutable.ArrayBuffer


final case class ObsStorageFileSystemProvider(config: ObsStorageConfiguration) extends FileSystemProvider {
  lazy val obsClient: ObsClient = config.newObsClient()

  class PathIterator(obsClient: ObsClient, prefix: ObsStoragePath, filter: DirectoryStream.Filter[_ >: Path]) extends AbstractIterator[Path] {
    var nextMarker: Option[String] = None

    var iterator: Iterator[String] = Iterator()

    override def computeNext(): Path = {
      if (!iterator.hasNext) {
        nextMarker match {
          case None => iterator = listNext("")
          case Some(marker: String) if !marker.isEmpty => iterator = listNext(marker)
          case Some(marker: String) if marker.isEmpty  => iterator = Iterator()
          case Some(null) => iterator = Iterator()
        }
      }


      if (iterator.hasNext) {
        val path = ObsStoragePath.getPath(prefix.getFileSystem, iterator.next())
        if (filter.accept(path)) {
          path
        } else {
          computeNext()
        }
      } else {
        endOfData()
      }
    }

    private[this] def listNext(marker: String): Iterator[String] = {
      val objectListing = ObsStorageRetry.from(
        () => {
          val listObjectRequest = new ListObjectsRequest(prefix.bucket)
          listObjectRequest.setDelimiter(UnixPath.SEPARATOR.toString)
          listObjectRequest.setPrefix(prefix.key)
          listObjectRequest.setMarker(marker)

          obsClient.listObjects(listObjectRequest)
        }
      )

      val result = ArrayBuffer.empty[String]

      objectListing.getObjects.asScala.filterNot(_.equals(prefix.key)).foreach(obj => {result append obj.getObjectKey.stripPrefix(prefix.key)})
      objectListing.getCommonPrefixes.asScala.filterNot(_.equals(prefix.key)).foreach(obj => {result append obj.stripPrefix(prefix.key)})

      nextMarker = Some(objectListing.getNextMarker)
      result.iterator
    }
  }

  class ObsStorageDirectoryStream(obsClient: ObsClient, prefix: ObsStoragePath, filter: DirectoryStream.Filter[_ >: Path]) extends DirectoryStream[Path] {

    override def iterator(): util.Iterator[Path] = new PathIterator(obsClient, prefix, filter)

    override def close(): Unit = {}

  }

  override def getScheme: String = ObsStorageFileSystem.URI_SCHEMA

  override def newFileSystem(uri: URI, env: util.Map[String, _]): ObsStorageFileSystem = {
    if (uri.getScheme != getScheme) {
      throw new IllegalArgumentException(s"Schema ${uri.getScheme} not match")
    }

    val bucket = uri.getHost
    if (bucket.isEmpty) {
      throw new IllegalArgumentException(s"Bucket is empty")
    }

    if (uri.getPort != -1) {
      throw new IllegalArgumentException(s"Port is not permitted")
    }

    ObsStorageFileSystem(this, bucket, ObsStorageConfiguration.parseMap(env.asScala.toMap))
  }

  override def getFileSystem(uri: URI): ObsStorageFileSystem = {
    newFileSystem(uri, config.toMap.asJava)
  }

  override def getPath(uri: URI): ObsStoragePath = {
    ObsStoragePath.getPath(getFileSystem(uri), uri.getPath)
  }

  override def newOutputStream(path: Path, options: OpenOption*): OutputStream = {
    if (!path.isInstanceOf[ObsStoragePath]) {
      throw new ProviderMismatchException(s"Not a obs storage path $path")
    }

    val len = options.length

    var opts = Set[OpenOption]()
    if (len == 0) {
      opts = Set(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      for (opt <- options) {
        if (opt == StandardOpenOption.READ) {
          throw new IllegalArgumentException("READ not allowed")
        }

        opts += opt
      }
    }

    opts += StandardOpenOption.WRITE
    val obsStream = ObsAppendOutputStream(obsClient, path.asInstanceOf[ObsStoragePath], true)

    new BufferedOutputStream(obsStream, 256*1024)
  }

  override def newByteChannel(path: Path, options: util.Set[_ <: OpenOption], attrs: FileAttribute[_]*): SeekableByteChannel = {
    if (!path.isInstanceOf[ObsStoragePath]) {
      throw new ProviderMismatchException(s"Not a obs storage path $path")
    }

    for (opt <- options.asScala) {
      opt match {
        case StandardOpenOption.READ =>
        case StandardOpenOption.WRITE => throw new IllegalArgumentException("WRITE byte channel not allowed currently")
        case StandardOpenOption.SPARSE | StandardOpenOption.TRUNCATE_EXISTING =>
        case StandardOpenOption.APPEND | StandardOpenOption.CREATE | StandardOpenOption.DELETE_ON_CLOSE |
             StandardOpenOption.CREATE_NEW | StandardOpenOption.DSYNC | StandardOpenOption.SYNC => throw new UnsupportedOperationException()
      }
    }

    ObsFileReadChannel(obsClient, 0, path.asInstanceOf[ObsStoragePath])
  }

  override def createDirectory(dir: Path, attrs: FileAttribute[_]*): Unit = {}

  override def deleteIfExists(path: Path): Boolean = {
    val obsPath = ObsStoragePath.checkPath(path)

    if (obsPath.seemsLikeDirectory) {
      if (headPrefix(obsPath)) {
        throw new UnsupportedOperationException("Can not delete a non-empty directory")
      }

      return true
    }

    val exist = ObsStorageRetry.from(
      () => {
        val request = new GetObjectMetadataRequest(obsPath.bucket, obsPath.key)
        try{
          obsClient.getObjectMetadata(request)
          true
        }catch {
          case ex: ObsException if ex.getResponseCode == 404 => false
        }
      }
    )

    if (!exist) {
      return false
    }

    ObsStorageRetry.from(
      () => obsClient.deleteObject(obsPath.bucket, obsPath.key)
    )

    true
  }

  override def delete(path: Path): Unit = {
    if (!deleteIfExists(path)) {
      throw new NoSuchFileException(s"File $path not exists")
    }
  }

  /*
   * XXX: Can only copy files whose size is below 1GB currently.
   */

  override def copy(source: Path, target: Path, options: CopyOption*): Unit = {
    val srcObsPath = ObsStoragePath.checkPath(source)
    val targetObsPath= ObsStoragePath.checkPath(target)

    // ignore all options currently.
    if (srcObsPath == targetObsPath) {
      return
    }

    val _ = ObsStorageRetry.from(
      () => obsClient.copyObject(srcObsPath.bucket, srcObsPath.key, targetObsPath.bucket, targetObsPath.key)
    )

  }

  override def move(source: Path, target: Path, options: CopyOption*): Unit = {
    copy(source, target, options: _*)

    val _ = deleteIfExists(source)
  }

  override def isSameFile(path: Path, path2: Path): Boolean = {
    ObsStoragePath.checkPath(path).equals(ObsStoragePath.checkPath(path2))
  }

  override def isHidden(path: Path): Boolean = {
    false
  }

  override def getFileStore(path: Path): FileStore = throw new UnsupportedOperationException()

  override def checkAccess(path: Path, modes: AccessMode*): Unit = {
    for (mode <- modes) {
      mode match {
        case AccessMode.READ | AccessMode.WRITE =>
        case AccessMode.EXECUTE => throw new AccessDeniedException(mode.toString)
      }
    }

    val obsPath = ObsStoragePath.checkPath(path)
    // directory always exists.
    if (obsPath.seemsLikeDirectory) {
      return
    }

    val exist = ObsStorageRetry.from(
      () => {
        val request = new GetObjectMetadataRequest(obsPath.bucket, obsPath.key)
        try{
          obsClient.getObjectMetadata(request)
          true
        }catch {
          case ex: ObsException if ex.getResponseCode == 404 => false
        }
      }
    )

    if (!exist) {
      throw new NoSuchFileException(path.toString)
    }
  }

  override def getFileAttributeView[V <: FileAttributeView](path: Path, `type`: Class[V], options: LinkOption*): V = {
    if (`type` != classOf[ObsStorageFileAttributesView] && `type` != classOf[BasicFileAttributeView] ) {
      throw new UnsupportedOperationException(`type`.getSimpleName)
    }

    val obsPath = ObsStoragePath.checkPath(path)

    ObsStorageFileAttributesView(obsClient, obsPath).asInstanceOf[V]
  }

  override def readAttributes(path: Path, attributes: String, options: LinkOption*): util.Map[String, AnyRef] = {
    throw new UnsupportedOperationException()
  }

  override def readAttributes[A <: BasicFileAttributes](path: Path, `type`: Class[A], options: LinkOption*): A = {
    if (`type` != classOf[ObsStorageFileAttributes] && `type` != classOf[BasicFileAttributes] ) {
      throw new UnsupportedOperationException(`type`.getSimpleName)
    }

    val obsPath = ObsStoragePath.checkPath(path)

    if (obsPath.seemsLikeDirectory) {
      return new ObsStorageDirectoryAttributes(obsPath).asInstanceOf[A]
    }

    val exists = ObsStorageRetry.from(
      () => {
        val request = new GetObjectMetadataRequest(obsPath.bucket, obsPath.key)
        try{
          obsClient.getObjectMetadata(request)
          true
        }catch {
          case ex: ObsException if ex.getResponseCode == 404 => false
        }
      }
    )

    if (!exists) {
      throw new NoSuchFileException(obsPath.toString)
    }

    val objectMeta = ObsStorageRetry.from(
      () => obsClient.getObjectMetadata(obsPath.bucket, obsPath.key)
    )

    ObsStorageObjectAttributes(objectMeta, obsPath).asInstanceOf[A]
  }

  override def newDirectoryStream(dir: Path, filter: DirectoryStream.Filter[_ >: Path]): DirectoryStream[Path] = {
    val obsPath = ObsStoragePath.checkPath(dir)

    new ObsStorageDirectoryStream(obsClient, obsPath, filter)
  }

  override def setAttribute(path: Path, attribute: String, value: scala.Any, options: LinkOption*): Unit = throw new UnsupportedOperationException()

  private[this] def headPrefix(path: Path): Boolean = {
    val obsPath = ObsStoragePath.checkPath(path)

    val listRequest = new ListObjectsRequest(obsPath.bucket)
    listRequest.setPrefix(obsPath.key)

    val listResult = ObsStorageRetry.from(
      () => obsClient.listObjects(listRequest)
    )

    listResult.getObjects.iterator().hasNext
  }
}
