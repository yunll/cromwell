package cromwell.filesystems.obs.nio

import java.io.{ByteArrayInputStream, OutputStream}

import com.obs.services.ObsClient
import com.obs.services.exception.ObsException
import com.obs.services.model.{AppendObjectRequest, GetObjectMetadataRequest}

import scala.util.Try

final case class ObsAppendOutputStream(obsClient: ObsClient, path: ObsStoragePath, deleteIfExists: Boolean) extends OutputStream {

  var position: Long = {
    val exist = ObsStorageRetry.fromTry(
      () => Try{
        val request = new GetObjectMetadataRequest(path.bucket, path.key)
        try{
          obsClient.getObjectMetadata(request)
          true
        }catch {
          case ex: ObsException if ex.getResponseCode == 404 => false
        }
      }
    )

    var len: Long = 0
    if (exist && deleteIfExists) {
      ObsStorageRetry.from(
        () => obsClient.deleteObject(path.bucket, path.key)
      )
    }
    else if (exist) {
      len = ObsStorageRetry.from(
        () => obsClient.getObjectMetadata(path.bucket, path.key).getContentLength
      )
    }

    len
  }

  override def write(b: Int): Unit = {
    val arr  = Array[Byte]((b & 0xFF).toByte)

    val appendObjectRequest: AppendObjectRequest = new AppendObjectRequest()
    appendObjectRequest.setBucketName(path.bucket)
    appendObjectRequest.setObjectKey(path.key)
    appendObjectRequest.setInput(new ByteArrayInputStream(arr))
    this.synchronized {
      appendObjectRequest.setPosition(position)
      val appendObjectResult = ObsStorageRetry.fromTry(
        () => Try{
          obsClient.appendObject(appendObjectRequest)
        }
      )

      position = appendObjectResult.getNextPosition()
    }
  }

  override def write(b: Array[Byte]): Unit = {
    val appendObjectRequest: AppendObjectRequest = new AppendObjectRequest()
    appendObjectRequest.setBucketName(path.bucket)
    appendObjectRequest.setObjectKey(path.key)
    appendObjectRequest.setInput(new ByteArrayInputStream(b))
    this.synchronized {
      appendObjectRequest.setPosition(position)
      val appendObjectResult = ObsStorageRetry.fromTry(
        () => Try{
          obsClient.appendObject(appendObjectRequest)
        }
      )
      position = appendObjectResult.getNextPosition()
    }
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (b == null) {
      throw new NullPointerException
    } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException
    } else if (len == 0) {
      return
    }

    val s = b.slice(off, off+len)
    val appendObjectRequest: AppendObjectRequest = new AppendObjectRequest()
    appendObjectRequest.setBucketName(path.bucket)
    appendObjectRequest.setObjectKey(path.key)
    appendObjectRequest.setInput(new ByteArrayInputStream(s))
    this.synchronized {
      appendObjectRequest.setPosition(position)
      val appendObjectResult = ObsStorageRetry.fromTry(
        () => Try{
          obsClient.appendObject(appendObjectRequest)
        }
      )
      position = appendObjectResult.getNextPosition()
    }
  }
}
