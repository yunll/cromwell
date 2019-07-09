package cromwell.filesystems.obs.nio

import java.nio.charset.Charset

import cromwell.core.TestKitSuite
import org.scalatest.{BeforeAndAfter}

import scala.util.Try
import scala.util.control.Breaks

object ObsFileReadChannelSpec {
  val FILENAME = "/test-obs-read-file"
  val CONTENT = "Hello World!"

  implicit class Crobsable[X](xs: Traversable[X]) {
    def crobs[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }
}

class ObsFileReadChannelSpec extends TestKitSuite with ObsNioUtilSpec with BeforeAndAfter {
  behavior of s"ObsFileReadChannelSpec"

  import ObsFileReadChannelSpec._


  def getPath = ObsStoragePath.getPath(obsFileSystem, FILENAME)

  before {
     Try(ObsAppendOutputStream(obsClient, getPath, true)) foreach {_.write(CONTENT.getBytes("UTF-8"))}
  }

  after {
    Try(deleteObject(getPath))
  }

  it should "has the right size" taggedAs NeedAK in {
    val channel = ObsFileReadChannel(obsClient, 0L, getPath)
    channel.size shouldEqual(CONTENT.length)
  }

  it should "has the right content" taggedAs NeedAK in {
    List.range(1, CONTENT.length + 1) foreach { bufferSize =>verifySameContent(bufferSize)}
    for (bufferSize <- List.range(1, CONTENT.length + 1); position <- List.range(0, CONTENT.length)) {
      verifySameContent(bufferSize, position.toLong)
    }
  }

  it should "has the right position after seeking" taggedAs NeedAK in {
    val channel = ObsFileReadChannel(obsClient, 0L, getPath)
    channel.size shouldEqual(CONTENT.length)

    channel.position(1)

    channel.position shouldEqual(1)
  }

  def verifySameContent(bufferSize: Int, position: Long = 0) = {
    val channel = ObsFileReadChannel(obsClient, position, getPath)

    import java.nio.ByteBuffer
    val buf = ByteBuffer.allocate(bufferSize)

    val loop = new Breaks
    val builder = new StringBuilder

    var bytesRead = channel.read(buf)
    loop.breakable {
      while (bytesRead != -1) {
        buf.flip()
        val charset = Charset.forName("UTF-8");

        builder.append(charset.decode(buf).toString())
        buf.clear
        bytesRead = channel.read(buf)
      }
    }

    builder.toString shouldEqual CONTENT.substring(position.toInt)
  }
}
