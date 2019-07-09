package cromwell.filesystems.obs.nio

import java.net.URI
import java.nio.charset.Charset
import java.nio.file.{DirectoryStream, NoSuchFileException, Path, StandardOpenOption}

import cromwell.core.TestKitSuite
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

class ObsStorageFileSystemProviderSpec extends TestKitSuite with ObsNioUtilSpec with BeforeAndAfter {
  behavior of "ObsStorageFileSystemProviderSpec"

  it should "has right schema" in {
    mockProvider.getScheme shouldEqual ObsStorageFileSystem.URI_SCHEMA
  }

  it should "work when creating new file system" in {
    val fs = mockProvider.newFileSystem(URI.create(s"obs://$bucket"), mockObsConf.toMap.asJava)
    fs.bucket shouldEqual bucket

    an [IllegalArgumentException] should be thrownBy obsProvider.newFileSystem(URI.create(s"obs://"), mockObsConf.toMap.asJava)
    an [IllegalArgumentException] should be thrownBy obsProvider.newFileSystem(URI.create(s"obs://$bucket:8812"), mockObsConf.toMap.asJava)

    val fs1 = mockProvider.getFileSystem(URI.create(s"obs://$bucket"))
    fs1.bucket shouldEqual bucket
  }

  it should "work when getting a new obs path" in {
    val path = mockProvider.getPath(URI.create(s"obs://$bucket$fileName"))
    path.bucket shouldEqual bucket
    path.key shouldEqual fileName.stripPrefix(ObsStorageFileSystem.SEPARATOR)
  }

  it should "work when creating an output stream" taggedAs NeedAK in {
    val path = obsProvider.getPath(URI.create(s"obs://$bucket$fileName"))

    val outS = obsProvider.newOutputStream(path)
    outS.write(fileContent.getBytes)

    contentAsString(path) shouldEqual fileContent
    outS.asInstanceOf[ObsAppendOutputStream].position shouldEqual fileContent.length
  }

  it should "work when creating an byte channel" taggedAs NeedAK in {
    val path = obsProvider.getPath(URI.create(s"obs://$bucket$fileName"))

    val outS = obsProvider.newOutputStream(path)
    outS.write(fileContent.getBytes)

    val inC = obsProvider.newByteChannel(path, Set(StandardOpenOption.READ).asJava)

    import java.nio.ByteBuffer
    val buf = ByteBuffer.allocate(1)

    val loop = new Breaks
    val builder = new StringBuilder

    var bytesRead = inC.read(buf)
    loop.breakable {
      while (bytesRead != -1) {
        buf.flip()
        val charset = Charset.forName("UTF-8")

        builder.append(charset.decode(buf).toString)
        buf.clear
        bytesRead = inC.read(buf)
      }
    }

    builder.toString shouldEqual fileContent
  }

  it should "delete file if it exists" taggedAs NeedAK in {
    val path = obsProvider.getPath(URI.create(s"obs://$bucket$fileName"))

    val outS = obsProvider.newOutputStream(path)
    outS.write(fileContent.getBytes)
    outS.close()

    obsProvider.deleteIfExists(path) shouldEqual true
    obsProvider.deleteIfExists(path) shouldEqual false
    an [NoSuchFileException] should be thrownBy obsProvider.delete(path)
  }

  it should "work when copying an object" taggedAs NeedAK in {
    val src = obsProvider.getPath(URI.create(s"obs://$bucket$fileName"))
    val target = obsProvider.getPath(URI.create(s"obs://$bucket${fileName}1"))
    obsProvider.deleteIfExists(src)

    writeObject(src)

    obsProvider.copy(src, target)

    obsProvider.deleteIfExists(target) shouldEqual true
  }

  it should "work when moving an object" taggedAs NeedAK in {
    val src = obsProvider.getPath(URI.create(s"obs://$bucket$fileName"))
    val target = obsProvider.getPath(URI.create(s"obs://$bucket${fileName}1"))
    obsProvider.deleteIfExists(src)

    writeObject(src)

    obsProvider.move(src, target)

    obsProvider.deleteIfExists(target) shouldEqual true
    obsProvider.deleteIfExists(src) shouldEqual false

  }

  it should "work for some basic operations" taggedAs NeedAK in {
    val path = obsProvider.getPath(URI.create(s"obs://$bucket$fileName"))
    val path1 = obsProvider.getPath(URI.create(s"obs://$bucket$fileName"))

    obsProvider.isHidden(path) shouldEqual false
    obsProvider.isSameFile(path, path1)

    an [UnsupportedOperationException] should be thrownBy obsProvider.getFileStore(path)

    an [NoSuchFileException] should be thrownBy obsProvider.checkAccess(path)

    val dir = obsProvider.getPath(URI.create(s"obs://$bucket${fileName}/"))
    noException should be thrownBy obsProvider.checkAccess(dir)
  }

  it should "work for attribute view" taggedAs NeedAK in {
    val path = obsProvider.getPath(URI.create(s"obs://$bucket$fileName"))
    obsProvider.deleteIfExists(path)

    writeObject(path)
    val view = obsProvider.getFileAttributeView(path, classOf[ObsStorageFileAttributesView])
    view shouldBe an [ObsStorageFileAttributesView]

    val attr = view.readAttributes()
    attr shouldBe an [ObsStorageObjectAttributes]

    val dir = obsProvider.getPath(URI.create(s"obs://$bucket${fileName}/"))
    val dirView = obsProvider.getFileAttributeView(dir, classOf[ObsStorageFileAttributesView])
    dirView shouldBe an [ObsStorageFileAttributesView]

    val dirAttr = dirView.readAttributes()
    dirAttr shouldBe an [ObsStorageDirectoryAttributes]
  }

  it should "work for reading attrs" taggedAs NeedAK in {
    val path = obsProvider.getPath(URI.create(s"obs://$bucket$fileName"))
    obsProvider.deleteIfExists(path)

    writeObject(path)
    val attr = obsProvider.readAttributes(path, classOf[ObsStorageFileAttributes])
    attr shouldBe an [ObsStorageObjectAttributes]

    obsProvider.deleteIfExists(path)
    a [NoSuchFileException] should be thrownBy obsProvider.readAttributes(path, classOf[ObsStorageFileAttributes])

    val dir = obsProvider.getPath(URI.create(s"obs://$bucket${fileName}/"))
    val dirAttr = obsProvider.readAttributes(dir, classOf[ObsStorageFileAttributes])
    dirAttr shouldBe an [ObsStorageDirectoryAttributes]
  }


  it should "work for reading dirs" taggedAs NeedAK in {
    val count = 10
    val testDir = "/test-read-dir"
    val filePrefix = "test-file"
    val expectedFileNames = ArrayBuffer.empty[String]
    val dir = obsProvider.getPath(URI.create(s"obs://$bucket$testDir/"))
    for (i <- 0 to count) {
      val fileName = filePrefix + i.toString
      expectedFileNames.append(fileName)

      val path = dir.resolve(fileName)

      obsProvider.deleteIfExists(path)
      writeObject(path)
    }

    val dirStream = obsProvider.newDirectoryStream(dir, new DirectoryStream.Filter[Path] {
      override def accept(entry: Path): Boolean = {
        true
      }
    })

    val files = ArrayBuffer.empty[String]
    dirStream.iterator.asScala foreach(file => files.append(file.toString))

    files should contain allElementsOf(expectedFileNames)
  }

}
