import java.io.FileInputStream

import cats.effect._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.StorageOptions

object IntermediateOutputs extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    /*
    List of arguments:
      - 0: working directory in GCS
      - 2: pointer to outputs file
     */

    //TODO: use scopt.OptionParser to parse command line arguments

    val workingDirectory = "<path_to_working_directory>"

    val workingDirectoryArray = workingDirectory.replace("gs://", "").split("/", 2)
    val serviceAccount = "<path_ to_sa>"

    val credentials = GoogleCredentials.fromStream(new FileInputStream(serviceAccount))
    val storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService
    val bucketObj = storage.get(workingDirectoryArray(0))


    // List all blobs in the bucket
    println("Blobs in the bucket:")
    bucketObj.list(BlobListOption.prefix(workingDirectoryArray(1))).iterateAll().forEach(println(_))

    IO(ExitCode(0))
  }
}
