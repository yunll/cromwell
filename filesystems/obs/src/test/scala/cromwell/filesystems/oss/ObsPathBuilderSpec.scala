package cromwell.filesystems.obs

import cromwell.core.TestKitSuite
import cromwell.filesystems.obs.nio.ObsNioUtilSpec
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}
import org.scalatest.TryValues._

class ObsPathBuilderSpec extends TestKitSuite with FlatSpecLike with Matchers with ObsNioUtilSpec with BeforeAndAfter {

  behavior of s"ObsPathBuilerSpec"
  val testPathBuiler = ObsPathBuilder(mockObsConf)

  it should "throw when no bucket in URI" in {
    testPathBuiler.build("obs:").failed.get shouldBe an[IllegalArgumentException]
    testPathBuiler.build("obs://").failed.get shouldBe an[IllegalArgumentException]
  }

  it should "throw when path has an invalid schema" in {
    testPathBuiler.build(s"gcs://$bucket$fileName").failed.get shouldBe an[IllegalArgumentException]
  }

  it should "has an empty key when no path specified" in {
    testPathBuiler.build(s"obs://$bucket").success.value.bucket shouldBe bucket
    testPathBuiler.build(s"obs://$bucket").success.value.key shouldBe empty
  }

  it should "start with separator when path specified" in {
    val path = testPathBuiler.build(s"obs://$bucket$fileName").success.value
    path.bucket shouldBe bucket
    path.nioPath.toString shouldBe fileName
    path.key shouldBe fileName.stripPrefix("/")
    path.pathAsString shouldBe s"obs://$bucket$fileName"
    path.pathWithoutScheme shouldBe s"$bucket$fileName"
  }
 }
