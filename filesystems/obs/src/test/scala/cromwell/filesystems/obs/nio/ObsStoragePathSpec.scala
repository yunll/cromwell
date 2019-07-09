package cromwell.filesystems.obs.nio

import cromwell.core.TestKitSuite
import scala.collection.JavaConverters._


class ObsStoragePathSpec extends TestKitSuite with ObsNioUtilSpec {
  behavior of s"ObsStoragePath"


  it should s"has the same bucket with file system" in {

    val path = ObsStoragePath.getPath(mockFileSystem, fileName)

    path.bucket shouldBe bucket

    path.toAbsolutePath.toString shouldBe fileName
  }

  it should s"has a separator-removed key" in {
    val path = ObsStoragePath.getPath(mockFileSystem, fileName)

    path.key shouldBe fileName.stripPrefix(UnixPath.SEPARATOR.toString)
  }

  "a not absolute obs path" should s"has a NullObsStoragePath root path" in {
    val path = ObsStoragePath.getPath(mockFileSystem, fileName.stripPrefix(UnixPath.SEPARATOR.toString))

    path.getRoot shouldBe a [NullObsStoragePath]
  }

  "an absolute obs path" should s"has a ObsStoragePathImpl root path" in {
    val path = ObsStoragePath.getPath(mockFileSystem, fileName)

    path.getRoot shouldBe an [ObsStoragePathImpl]
  }

  it should s"has right iterator" in {
    val path = ObsStoragePath.getPath(mockFileSystem, fileName)

    var subs = List.empty[String]
    path.iterator().asScala foreach(p => subs = subs :+ p.toString)

    subs.head shouldBe "bcs-dir"
    subs(1) shouldBe "bcs-file"
  }

  it should s"has right relativize" in {
    val path = ObsStoragePath.getPath(mockFileSystem, fileName)

    val path1 = ObsStoragePath.getPath(mockFileSystem, "/bcs-dir/bcs-file1")

    path.relativize(path1).toString shouldEqual "../bcs-file1"

    val path2 = ObsStoragePath.getPath(mockFileSystem, "/bcs-dir1/bcs-file2")
    path.relativize(path2).toString shouldEqual "../../bcs-dir1/bcs-file2"
  }

  it should s"has right pathAsString" in {
    val path = ObsStoragePath.getPath(mockFileSystem, fileName)

    path.pathAsString shouldEqual s"obs://$bucket$fileName"
  }

}
