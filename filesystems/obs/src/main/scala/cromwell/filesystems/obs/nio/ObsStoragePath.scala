package cromwell.filesystems.obs.nio

import java.io.File
import java.net.URI
import java.nio.file._
import java.util
import java.util.Objects

import com.google.common.collect.UnmodifiableIterator

object ObsStoragePath {
  def checkObsStoragePath(other: Path): ObsStoragePath = {
    if (!other.isInstanceOf[ObsStoragePath]) {
      throw new ProviderMismatchException(s"Not a obs storage path $other")
    }

    other.asInstanceOf[ObsStoragePath]
  }

  def getPath(filesystem: ObsStorageFileSystem, path: UnixPath) = new ObsStoragePathImpl(filesystem, path)

  def getPath(filesystem: ObsStorageFileSystem, first: String, more: String*) = new ObsStoragePathImpl(filesystem, UnixPath.getPath(first, more: _*))

  def checkPath(path: Path): ObsStoragePath = {
    if (!path.isInstanceOf[ObsStoragePath]) {
      throw new ProviderMismatchException(s"Not an obs storage path $path")
    }

    path.asInstanceOf[ObsStoragePath]
  }

}

trait ObsStoragePath extends Path {
  def bucket = ""

  def key = ""

  def path: UnixPath = UnixPath.EMPTY_PATH

  def seemsLikeDirectory = false

  def pathAsString: String = ""

  override def getFileSystem: ObsStorageFileSystem = throw new UnsupportedOperationException

  override def isAbsolute: Boolean = throw new UnsupportedOperationException

  override def getRoot: ObsStoragePath = throw new UnsupportedOperationException

  override def getFileName: ObsStoragePath = throw new UnsupportedOperationException

  override def getParent: ObsStoragePath = throw new UnsupportedOperationException

  override def getNameCount: Int = throw new UnsupportedOperationException

  override def getName(index: Int): ObsStoragePath = throw new UnsupportedOperationException

  override def subpath(beginIndex: Int, endIndex: Int): ObsStoragePath = throw new UnsupportedOperationException

  override def startsWith(other: Path): Boolean = throw new UnsupportedOperationException

  override def startsWith(other: String): Boolean = throw new UnsupportedOperationException

  override def endsWith(other: Path): Boolean = throw new UnsupportedOperationException

  override def endsWith(other: String): Boolean = throw new UnsupportedOperationException

  override def normalize(): ObsStoragePath = throw new UnsupportedOperationException

  override def resolve(other: Path): ObsStoragePath = throw new UnsupportedOperationException

  override def resolve(other: String): ObsStoragePath = throw new UnsupportedOperationException

  override def resolveSibling(other: Path): ObsStoragePath = throw new UnsupportedOperationException

  override def resolveSibling(other: String): ObsStoragePath = throw new UnsupportedOperationException

  override def relativize(other: Path): ObsStoragePath = throw new UnsupportedOperationException

  override def toAbsolutePath: ObsStoragePath = throw new UnsupportedOperationException

  override def toRealPath(options: LinkOption*): ObsStoragePath = throw new UnsupportedOperationException

  override def toFile: File = throw new UnsupportedOperationException

  override def register(watcher: WatchService, events: WatchEvent.Kind[_]*): WatchKey = throw new UnsupportedOperationException

  override def register(watcher: WatchService, events: Array[WatchEvent.Kind[_]], modifiers: WatchEvent.Modifier*): WatchKey = throw new UnsupportedOperationException

  override def iterator(): util.Iterator[Path] = throw new UnsupportedOperationException

  override def compareTo(other: Path): Int = throw new UnsupportedOperationException

  override def toUri: URI = throw new UnsupportedOperationException
}

final case class ObsStoragePathImpl(filesystem: ObsStorageFileSystem, override val path: UnixPath = UnixPath.EMPTY_PATH) extends  ObsStoragePath {

  override def pathAsString: String = toUri.toString

  override def bucket: String = filesystem.bucket

  override def key: String = toAbsolutePath.toString.stripPrefix("/")

  override def getFileSystem: ObsStorageFileSystem = filesystem

  override def isAbsolute: Boolean = path.isAbsolute

  override def getRoot: ObsStoragePath = path.getRoot map {path => newPath(path)} getOrElse NullObsStoragePath(filesystem)

  override def getFileName: ObsStoragePath = path.getFileName map {path => newPath(path)} getOrElse NullObsStoragePath(filesystem)

  override def getParent: ObsStoragePath = path.getParent map {path => newPath(path)} getOrElse NullObsStoragePath(filesystem)

  override def getNameCount: Int = path.getNameCount

  override def getName(index: Int): ObsStoragePath = path.getName(index) map {path => newPath(path)} getOrElse NullObsStoragePath(filesystem)

  override def subpath(beginIndex: Int, endIndex: Int): ObsStoragePath = path.subPath(beginIndex, endIndex) map {path => newPath(path)} getOrElse NullObsStoragePath(filesystem)

  override def startsWith(other: Path): Boolean = {
    if (!other.isInstanceOf[ObsStoragePath]) {
      return false
    }

    val that = other.asInstanceOf[ObsStoragePath]
    if (bucket != that.bucket) {
      return false
    }

    path.startsWith(that.path)
  }

  override def startsWith(other: String): Boolean = {
    path.startsWith(UnixPath.getPath(other))
  }

  override def endsWith(other: Path): Boolean = {
    if (!other.isInstanceOf[ObsStoragePath]) {
      return false
    }
    val that = other.asInstanceOf[ObsStoragePath]
    if (bucket != that.bucket) {
      return false
    }

    path.endsWith(that.path)
  }

  override def endsWith(other: String): Boolean = {
    path.endsWith(UnixPath.getPath(other))
  }

  override def normalize(): ObsStoragePath = newPath(path.normalize())

  override def resolve(other: Path): ObsStoragePath = {
    val that = ObsStoragePath.checkObsStoragePath(other)

    newPath(path.resolve(that.path))
  }

  override def resolve(other: String): ObsStoragePath = {
    newPath(path.resolve(UnixPath.getPath(other)))
  }

  override def resolveSibling(other: Path): ObsStoragePath = {
    val that = ObsStoragePath.checkObsStoragePath(other)

    newPath(path.resolveSibling(that.path))
  }

  override def resolveSibling(other: String): ObsStoragePath = {
    newPath(path.resolveSibling(UnixPath.getPath(other)))
  }

  override def relativize(other: Path): ObsStoragePath = {
    val that = ObsStoragePath.checkObsStoragePath(other)

    newPath(path.relativize(that.path))
  }

  /**
   * currently a mocked one
   */
  override def toAbsolutePath: ObsStoragePath = {
    newPath(path.toAbsolutePath())
  }

  override def toRealPath(options: LinkOption*): ObsStoragePath = toAbsolutePath

  override def toFile: File = throw new UnsupportedOperationException

  override def register(watcher: WatchService, events: WatchEvent.Kind[_]*): WatchKey = throw new UnsupportedOperationException

  override def register(watcher: WatchService, events: Array[WatchEvent.Kind[_]], modifiers: WatchEvent.Modifier*): WatchKey = throw new UnsupportedOperationException

  override def iterator(): util.Iterator[Path] = {
    if (path.isEmpty || path.isRoot) {
      return util.Collections.emptyIterator()
    }

    new PathIterator()
  }

  override def compareTo(other: Path): Int = {
    if (other.isInstanceOf[ObsStoragePath]) {
      return -1
    }

    val that = other.asInstanceOf[ObsStoragePath]
    val res: Int = bucket.compareTo(that.bucket)
    if (res != 0) {
      return res
    }

    path.compareTo(that.path)
  }

  override def seemsLikeDirectory = path.seemsLikeDirectory()

  override def equals(obj: scala.Any): Boolean = {
    (this eq obj.asInstanceOf[AnyRef]) || obj.isInstanceOf[ObsStoragePath] && obj.asInstanceOf[ObsStoragePath].bucket.equals(bucket) && obj.asInstanceOf[ObsStoragePath].path.equals(path)
  }

  override def hashCode(): Int = {
    Objects.hash(bucket, toAbsolutePath.toString)
  }

  override def toString: String = path.toString

  override def toUri: URI = new URI("obs", bucket, toAbsolutePath.toString, None.orNull)

  private[this] def newPath(unixPath: UnixPath): ObsStoragePath = {
    if (unixPath == path) {
      this
    } else {
      ObsStoragePathImpl(filesystem, unixPath)
    }
  }


  class PathIterator extends UnmodifiableIterator[Path] {
    val delegate = path.split()

    override def next(): ObsStoragePath = newPath(UnixPath.getPath(delegate.next()))

    override def hasNext: Boolean = delegate.hasNext
  }
}

final case class NullObsStoragePath(filesystem: ObsStorageFileSystem) extends ObsStoragePath
