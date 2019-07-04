package cromwell.filesystems.obs.nio

import cromwell.core.TestKitSuite

class ObsStorageFileSystemSpec extends TestKitSuite with ObsNioUtilSpec {
  behavior of s"ObsStorageFileSystemSpec"

  it should "get right path" in {
    val obsPath = mockFileSystem.getPath("/test-file-system")
    obsPath.bucket shouldEqual(bucket)
    obsPath.key shouldEqual("test-file-system")
  }

  it should "has right view name" in {
    val fs = mockFileSystem

    fs.supportedFileAttributeViews should contain (ObsStorageFileSystem.BASIC_VIEW)
    fs.supportedFileAttributeViews should contain (ObsStorageFileSystem.OBS_VIEW)
  }

  it should "do not support some method" in {
    an [UnsupportedOperationException] should be thrownBy mockFileSystem.newWatchService
  }

  it should "return some expected simple mocked result" in {
    mockFileSystem.isOpen shouldBe true
    mockFileSystem.isReadOnly shouldBe false
    mockFileSystem.getSeparator shouldBe ObsStorageFileSystem.SEPARATOR
  }
}
