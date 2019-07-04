package cromwell.filesystems.obs.nio

import java.nio.ByteBuffer
import java.nio.channels.{Channels, SeekableByteChannel}

import com.obs.services.model.{GetObjectMetadataRequest, GetObjectRequest}
import com.obs.services.ObsClient

import scala.util.Try

final case class ObsFileReadChannel(obsClient: ObsClient, pos: Long, path: ObsStoragePath) extends ObsFileChannel {
  var internalPosition = pos

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

    if (begin >= size) {
        return -1
    }

    if (end >= size()) {
      end = size() - 1
    }

    val getObjectRequest = new GetObjectRequest(path.bucket, path.key)
    getObjectRequest.setRangeStart(begin)
    getObjectRequest.setRangeEnd(end)

    ObsStorageRetry.fromTry(
      () => Try{
        val obsObject = obsClient.getObject(getObjectRequest)
        val in = obsObject.getObjectContent
        val channel = Channels.newChannel(in)

        val amt = channel.read(dst)
        internalPosition += amt
        amt
      }
    )
  }

  override def size(): Long = {
    ObsStorageRetry.fromTry(
      () => Try {
        val request = new GetObjectMetadataRequest(path.bucket, path.key)
        obsClient.getObjectMetadata(request).getContentLength
      }
    )
  }
}
