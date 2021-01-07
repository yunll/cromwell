package cromwell.filesystems.obs.nio

import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, SeekableByteChannel}

import com.obs.services.model.GetObjectMetadataRequest
import com.obs.services.ObsClient

import scala.util.Try

final case class ObsFileReadChannel(obsClient: ObsClient, pos: Long, path: ObsStoragePath) extends ObsFileChannel {
  var internalPosition = pos

  var total = size

  val channel = getChannel()

  override def position(): Long = {
    synchronized {
      internalPosition
    }
  }

  override def position(newPosition: Long): SeekableByteChannel = {
    if (newPosition < 0) {
      throw new IllegalArgumentException(newPosition.toString)
    }

    synchronized {
      if (newPosition != internalPosition) {
        internalPosition = newPosition
      }

      return this
    }
  }

  override def read(dst: ByteBuffer): Int = {
    dst.mark()

    dst.reset()

    val want = dst.capacity
    val begin: Long = position()
    var end: Long = position + want - 1
    if (begin < 0 || end < 0 || begin > end) {
      throw new IllegalArgumentException(s"being $begin or end $end invalid")
    }

    if (begin >= total) {
        return -1
    }

    if (end >= total) {
       end = total - 1
    }

    val amt = channel.read(dst)
    internalPosition += amt
    amt
  }

  private def getChannel(): ReadableByteChannel = {
    ObsStorageRetry.fromTry(
      () => Try {
        val obsObject = obsClient.getObject(path.bucket, path.key)
        val in = obsObject.getObjectContent
        Channels.newChannel(in)
      }
    )
  }

  override def size(): Long = {
    ObsStorageRetry.fromTry(
      () => Try {
        val request = new GetObjectMetadataRequest(path.bucket, path.key)
        val meta = obsClient.getObjectMetadata(request)
        meta.getContentLength
      }
    )
  }
}
