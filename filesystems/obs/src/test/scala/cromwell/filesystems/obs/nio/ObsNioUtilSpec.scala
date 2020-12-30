package cromwell.filesystems.obs.nio

import java.io.ByteArrayInputStream

import com.obs.services.ObsClient
import org.scalatest._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.util.control.Breaks
import scala.util.{Failure, Success, Try}

object NeedAK extends Tag("this test need obs storage access id and key")

object ObsNioUtilSpec {
  val DEFAULT_BUCKET = "bcs-bucket"

  val DEFAULT_FILE_NAME = "/bcs-dir/bcs-file"

  val DEFAULT_CONTENT = "Hello World!"

  val obsInfo: Map[String, String] = Map(
    "endpoint" -> "",
    "access-id" -> "",
    "access-key" -> "",
    "bucket" -> DEFAULT_BUCKET
  )
}

trait ObsNioUtilSpec extends AnyFlatSpecLike with MockitoSugar with Matchers {

  override def withFixture(test: NoArgTest): Outcome = {
    if (test.tags.contains(NeedAK.name)) {
      Try(obsConf) match {
        case Success(_) => super.withFixture(test)
        case Failure(_) => cancel(NeedAK.name)
      }
    } else {
      super.withFixture(test)
    }
  }

  import ObsNioUtilSpec._

  lazy val bucket: String = {
    val bucket = obsInfo.getOrElse("bucket", "mock-bucket")
    if (bucket.isEmpty) {
      throw new IllegalArgumentException("test bucket can not be empty")
    }

    bucket
  }

  lazy val obsConf: ObsStorageConfiguration = Try{
    ObsStorageConfiguration.parseMap(obsInfo)
  } getOrElse(throw new IllegalArgumentException("you should supply obs info before testing obs related operation"))

  lazy val mockObsConf: ObsStorageConfiguration = new ObsStorageConfiguration("mock-endpoint", "mock-key", "mock-security")

  lazy val obsProvider = ObsStorageFileSystemProvider(obsConf)
  lazy val mockProvider = ObsStorageFileSystemProvider(mockObsConf)
  lazy val obsFileSystem = ObsStorageFileSystem(bucket, obsConf)
  lazy val mockFileSystem = ObsStorageFileSystem(bucket, mockObsConf)
  val fileName = DEFAULT_FILE_NAME
  val fileContent = DEFAULT_CONTENT

  lazy val obsClient: ObsClient = mockObsConf.newObsClient()

  def contentAsString(path: ObsStoragePath): String = {
    val obsObject = obsClient.getObject(path.bucket, path.key)

    val in = ObsStorageRetry.from(
      () => obsObject.getObjectContent
    )

    val maxLen = 1024
    val loop = new Breaks
    val result = new StringBuilder
    loop.breakable {
      while(true) {
        val b = new Array[Byte](maxLen)
        val got = ObsStorageRetry.from(
          () => in.read(b, 0, maxLen)
        )
        if (got <= 0) {
          loop.break()
        }
        result.append(new String(b, 0, got))
      }
    }

    result.toString()
  }

  def deleteObject(path: ObsStoragePath): Unit = {
    ObsStorageRetry.from(
      () => obsClient.deleteObject(path.bucket, path.key)
    )
    ()
  }

  def writeObject(path: ObsStoragePath): Unit = {
    ObsStorageRetry.from{
      () => obsClient.putObject(path.bucket, path.key, new ByteArrayInputStream(fileContent.getBytes()))
    }
    ()
  }
}
