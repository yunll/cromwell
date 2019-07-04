package cromwell.filesystems.obs.nio


import java.nio.file._
import java.nio.file.attribute.UserPrincipalLookupService
import java.util.Objects
import java.{lang, util}

import com.obs.services.ObsClient

import scala.collection.JavaConverters._


object ObsStorageFileSystem {
  val SEPARATOR: String = "/"
  val URI_SCHEMA: String = "obs"
  val OBS_VIEW = "obs"
  val BASIC_VIEW = "basic"

  def apply(provider: ObsStorageFileSystemProvider, bucket: String, config: ObsStorageConfiguration): ObsStorageFileSystem = {
    val res = new ObsStorageFileSystem(bucket, config)
    res.internalProvider = provider
    res
  }
}

object ObsStorageConfiguration {
  val ENDPOINT_KEY = "endpoint"
  val ACCESS_KEY_KEY = "access-key"
  val SECURITY_TOKEN_KEY = "security-key"

  import scala.collection.immutable.Map
  def parseMap(map: Map[String, Any]): ObsStorageConfiguration = {
    val endpoint = map.get(ENDPOINT_KEY) match {
      case Some(endpoint: String) if !endpoint.isEmpty => endpoint
      case _ => throw new IllegalArgumentException(s"endpoint is mandatory and must be an unempty string")
    }

    val accessKey = map.get(ACCESS_KEY_KEY) match {
      case Some(key: String) if !key.isEmpty => key
      case _ => throw new IllegalArgumentException(s"access-key is mandatory and must be an unempty string")
    }

    val securityKey = map.get(SECURITY_TOKEN_KEY) match {
      case Some(token: String) if !token.isEmpty => token
      case _ => throw new IllegalArgumentException(s"security-key is mandatory and must be an unempty string")
    }

    new ObsStorageConfiguration(endpoint, accessKey, securityKey)
  }

  def getClient(map: Map[String, String]): ObsClient = {
    parseMap(map).newObsClient()
  }

  def getClient(endpoint: String,
                accessKey: String,
                securityKey: String): ObsClient = {
    ObsStorageConfiguration(endpoint, accessKey, securityKey).newObsClient()
  }

}

final case class ObsStorageConfiguration(endpoint: String,
                                   accessKey: String,
                                   securityKey: String
                                  ) {
  import ObsStorageConfiguration._

  def toMap: Map[String, String] = {
    Map(ENDPOINT_KEY -> endpoint,  ACCESS_KEY_KEY -> accessKey, SECURITY_TOKEN_KEY -> securityKey)
  }

  def newObsClient() = {
    val obsClient = new ObsClient(endpoint)
    obsClient.refresh(accessKey,securityKey, "")
    obsClient
  }
}

case class ObsStorageFileSystem(bucket: String, config: ObsStorageConfiguration) extends FileSystem {

  var internalProvider: ObsStorageFileSystemProvider = ObsStorageFileSystemProvider(config)

  override def provider: ObsStorageFileSystemProvider = internalProvider

  override def getPath(first: String, more: String*): ObsStoragePath = ObsStoragePath.getPath(this, first, more: _*)

  override def close(): Unit = {
    // do nothing currently.
  }

  override def isOpen: Boolean = {
    true
  }

  override def isReadOnly: Boolean = {
    false
  }

  override def getSeparator: String = {
    ObsStorageFileSystem.SEPARATOR
  }

  override def getRootDirectories: lang.Iterable[Path] = {
    Set[Path](ObsStoragePath.getPath(this, UnixPath.ROOT_PATH)).asJava
  }

  override def getFileStores: lang.Iterable[FileStore] = {
    Set.empty[FileStore].asJava
  }

  override def getPathMatcher(syntaxAndPattern: String): PathMatcher = {
    FileSystems.getDefault.getPathMatcher(syntaxAndPattern)
  }

  override def getUserPrincipalLookupService: UserPrincipalLookupService = {
    throw new UnsupportedOperationException()
  }

  override def newWatchService(): WatchService = {
    throw new UnsupportedOperationException()
  }

  override def supportedFileAttributeViews(): util.Set[String] = {
    Set(ObsStorageFileSystem.OBS_VIEW, ObsStorageFileSystem.BASIC_VIEW).asJava
  }

  override def equals(obj: scala.Any): Boolean = {
    this == obj ||
      obj.isInstanceOf[ObsStorageFileSystem] &&
        obj.asInstanceOf[ObsStorageFileSystem].config.equals(config) &&
        obj.asInstanceOf[ObsStorageFileSystem].bucket.equals(bucket)
  }

  override def hashCode(): Int = Objects.hash(bucket)

  override def toString: String = ObsStorageFileSystem.URI_SCHEMA + "://" + bucket
}

