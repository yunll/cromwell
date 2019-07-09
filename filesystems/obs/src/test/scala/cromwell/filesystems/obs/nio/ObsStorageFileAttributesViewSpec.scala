package cromwell.filesystems.obs.nio

import com.obs.services.ObsClient
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._


class ObsStorageFileAttributesViewSpec extends ObsNioUtilSpec {
  behavior of "ObsStorageFileAttributesView"

  import ObsStorageObjectAttributesSpec._

  def getObject = {
    ObsStoragePath.getPath(mockFileSystem, fileName)
  }

  def getDir = {
    ObsStoragePath.getPath(mockFileSystem, "/bcs-dir/")
  }

  it should "return an object attr" in {
    val obsClient = mock[ObsClient]
    val meta = getObjectMeta
    when(obsClient.getObjectMetadata(anyString(), anyString())).thenReturn(meta)

    val view = ObsStorageFileAttributesView(obsClient, getObject)
    view.readAttributes shouldBe an [ObsStorageObjectAttributes]
  }

  it should "return an dir attr" in {
    val obsClient = mock[ObsClient]
    val view = ObsStorageFileAttributesView(obsClient, getDir)
    view.readAttributes shouldBe a [ObsStorageDirectoryAttributes]
  }

}
