package cromwell.filesystems.obs.nio

import cromwell.core.TestKitSuite

class ObsAppendOutputStreamSpec extends TestKitSuite with ObsNioUtilSpec  {

  behavior of s"ObsAppendOutputStream"

  "write batch" should "work" taggedAs NeedAK in {
    val path = ObsStoragePath.getPath(obsFileSystem, "/test-obs-append")
    val stream = ObsAppendOutputStream(obsClient, path, true)

    val content: String = "haha"
    stream.write(content.getBytes)

    contentAsString(path) shouldEqual content
    stream.position shouldEqual content.length
  }

  "write single" should "work" taggedAs NeedAK in {
    val c: Char = 'c'
    val path = ObsStoragePath.getPath(obsFileSystem, "/test-obs-append")
    val stream = ObsAppendOutputStream(obsClient, path, true)

    stream.write(c.toInt)

    contentAsString(path) shouldEqual c.toString
    stream.position shouldEqual 1
  }

  "write range" should "work" taggedAs NeedAK in {
    val path = ObsStoragePath.getPath(obsFileSystem, "/test-obs-append")
    val stream = ObsAppendOutputStream(obsClient, path, true)

    val content: String = "haha"
    stream.write(content.getBytes, 1, 1)

    contentAsString(path) shouldEqual 'a'.toString
    stream.position shouldEqual 1
  }

}
