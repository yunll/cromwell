package cromwell.filesystems.obs

import java.net.URI

import com.google.common.net.UrlEscapers
import cromwell.core.WorkflowOptions
import cromwell.core.path.{NioPath, Path, PathBuilder}
import cromwell.filesystems.obs.ObsPathBuilder._
import cromwell.filesystems.obs.nio.{ObsStorageConfiguration, ObsStorageFileSystem, ObsStoragePath}

import scala.language.postfixOps
import scala.util.matching.Regex
import scala.util.{Failure, Try}

object ObsPathBuilder {

  val URI_SCHEME = ObsStorageFileSystem.URI_SCHEMA

  val ObsBucketPattern:Regex =
    """
      (?x)                                      # Turn on comments and whitespace insensitivity
      ^obs://
      (                                         # Begin capturing group for obs bucket name
        [a-z0-9][a-z0-9-_\\.]+[a-z0-9]          # Regex for bucket name - soft validation, see comment above
      )                                         # End capturing group for gcs bucket name
      (?:
        /.*                                     # No validation here
      )?
    """.trim.r

  sealed trait ObsPathValidation

  case class ValidFullObsPath(bucket: String, path: String) extends ObsPathValidation

  case object PobsiblyValidRelativeObsPath extends ObsPathValidation

  sealed trait InvalidObsPath extends ObsPathValidation {
    def pathString: String
    def errorMessage: String
  }

  final case class InvalidScheme(pathString: String) extends InvalidObsPath {
    def errorMessage = s"OBS URIs must have 'obs' scheme: $pathString"
  }

  final case class InvalidFullObsPath(pathString: String) extends InvalidObsPath {
    def errorMessage = {
      s"""
         |The path '$pathString' does not seem to be a valid OBS path.
         |Please check that it starts with obs:// and that the bucket and object follow OBS naming guidelines.
      """.stripMargin.replaceAll("\n", " ").trim
    }
  }

  final case class UnparseableObsPath(pathString: String, throwable: Throwable) extends InvalidObsPath {
    def errorMessage: String =
      List(s"The specified OBS path '$pathString' does not parse as a URI.", throwable.getMessage).mkString("\n")
  }

  private def softBucketParsing(string: String): Option[String] = string match {
    case ObsBucketPattern(bucket) => Option(bucket)
    case _ => None
  }

  def validateObsPath(string: String): ObsPathValidation = {
    Try {
      val uri = URI.create(UrlEscapers.urlFragmentEscaper().escape(string))
      if (uri.getScheme == null) PobsiblyValidRelativeObsPath
      else if (uri.getScheme.equalsIgnoreCase(URI_SCHEME)) {
        if (uri.getHost == null) {
          softBucketParsing(string) map { ValidFullObsPath(_, uri.getPath) } getOrElse InvalidFullObsPath(string)
        } else ValidFullObsPath(uri.getHost, uri.getPath)
      } else InvalidScheme(string)
    } recover { case t => UnparseableObsPath(string, t) } get
  }

  def isObsPath(nioPath: NioPath): Boolean = {
    nioPath.getFileSystem.provider().getScheme.equalsIgnoreCase(URI_SCHEME)
  }

  def fromConfiguration(endpoint: String,
                        accessKey: String,
                        securityKey: String,
                        options: WorkflowOptions): ObsPathBuilder = {

    val configuration = ObsStorageConfiguration(endpoint, accessKey, securityKey)

    ObsPathBuilder(configuration)
  }
}

final case class ObsPathBuilder(obsStorageConfiguration: ObsStorageConfiguration) extends PathBuilder {
  def build(string: String): Try[ObsPath] = {
    validateObsPath(string) match {
      case ValidFullObsPath(bucket, path) =>
        Try {
          val nioPath = ObsStorageFileSystem(bucket, obsStorageConfiguration).getPath(path)
          ObsPath(nioPath)
        }
      case PobsiblyValidRelativeObsPath => Failure(new IllegalArgumentException(s"$string does not have a obs scheme"))
      case invalid: InvalidObsPath => Failure(new IllegalArgumentException(invalid.errorMessage))
    }
  }

  override def name: String = "Object Storage Service"
}

final case class BucketAndObj(bucket: String, obj: String)

final case class ObsPath private[obs](nioPath: NioPath) extends Path {

  override protected def newPath(path: NioPath): ObsPath = {
    ObsPath(path)
  }

  override def pathAsString: String = obsStoragePath.pathAsString

  override def pathWithoutScheme: String = {
    obsStoragePath.bucket + obsStoragePath.toAbsolutePath.toString
  }

  def bucket: String = {
    obsStoragePath.bucket
  }

  def key: String = {
    obsStoragePath.key
  }

  def obsStoragePath: ObsStoragePath = nioPath match {
    case obsPath: ObsStoragePath => obsPath
    case _ => throw new RuntimeException(s"Internal path was not a cloud storage path: $nioPath")
  }
}
