package cromwell.filesystems.obs.nio

import java.io.{ByteArrayInputStream, OutputStream}

import com.obs.services.ObsClient
import com.obs.services.exception.ObsException
import com.obs.services.model.fs.{FSStatusEnum, GetBucketFSStatusRequest}
import com.obs.services.model.{AppendObjectRequest, GetObjectMetadataRequest, ModifyObjectRequest}

import scala.util.Try

final case class ObsAppendOutputStream(obsClient: ObsClient, path: ObsStoragePath, deleteIfExists: Boolean) extends OutputStream {

  var position: Long = ObsStorageRetry.fromTry(
    () => Try{
      val request = new GetObjectMetadataRequest(path.bucket, path.key)
      try{
        val metadata = obsClient.getObjectMetadata(request)
        if (deleteIfExists){
          obsClient.deleteObject(path.bucket, path.key)
          0
        } else {
          metadata.getContentLength
        }
      }catch {
        case ex: ObsException if ex.getResponseCode == 404 => 0
      }
    }
  )

  var fsType: Boolean = ObsStorageRetry.fromTry(
    () => Try{
      val request = new GetBucketFSStatusRequest(path.bucket)
      try{
        val status = obsClient.getBucketFSStatus(request)
        status.getStatus == FSStatusEnum.ENABLED
      }catch {
        case ex: ObsException if ex.getResponseCode == 404 => false
      }
    }
  )

  override def write(b: Int): Unit = {
    val arr  = Array[Byte]((b & 0xFF).toByte)
    if (fsType) {
      val request: ModifyObjectRequest = new ModifyObjectRequest(path.bucket, path.key, new ByteArrayInputStream(arr))
      this.synchronized {
        request.setPosition(position)
        ObsStorageRetry.fromTry(
          () => Try {
            obsClient.modifyObject(request)
          }
        )
        position += arr.length
      }
    } else {
      val request: AppendObjectRequest = new AppendObjectRequest()
      request.setBucketName(path.bucket)
      request.setObjectKey(path.key)
      request.setInput(new ByteArrayInputStream(arr))
      this.synchronized {
        request.setPosition(position)
        val result = ObsStorageRetry.fromTry(
          () => Try {
            obsClient.appendObject(request)
          }
        )
        position = result.getNextPosition()
      }
    }
  }

  override def write(b: Array[Byte]): Unit = {
    if (fsType) {
      val request: ModifyObjectRequest = new ModifyObjectRequest(path.bucket, path.key, new ByteArrayInputStream(b))
      this.synchronized {
        request.setPosition(position)
        ObsStorageRetry.fromTry(
          () => Try {
            obsClient.modifyObject(request)
          }
        )
        position += b.length
      }
    } else {
      val request: AppendObjectRequest = new AppendObjectRequest()
      request.setBucketName(path.bucket)
      request.setObjectKey(path.key)
      request.setInput(new ByteArrayInputStream(b))
      this.synchronized {
        request.setPosition(position)
        val result = ObsStorageRetry.fromTry(
          () => Try {
            obsClient.appendObject(request)
          }
        )
        position = result.getNextPosition()
      }
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

    val arr = b.slice(off, off+len)
    if (fsType) {
      val request: ModifyObjectRequest = new ModifyObjectRequest(path.bucket, path.key, new ByteArrayInputStream(arr))
      this.synchronized {
        request.setPosition(position)
        ObsStorageRetry.fromTry(
          () => Try {
            obsClient.modifyObject(request)
          }
        )
        position += arr.length
      }
    } else {
      val request: AppendObjectRequest = new AppendObjectRequest()
      request.setBucketName(path.bucket)
      request.setObjectKey(path.key)
      request.setInput(new ByteArrayInputStream(arr))
      this.synchronized {
        request.setPosition(position)
        val result = ObsStorageRetry.fromTry(
          () => Try {
            obsClient.appendObject(request)
          }
        )
        position = result.getNextPosition()
      }
    }
  }
}
