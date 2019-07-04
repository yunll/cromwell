package cromwell.filesystems.obs.nio

import com.obs.services.exception.ObsException
import common.util.Backoff
import cromwell.core.retry.SimpleExponentialBackoff

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object ObsStorageRetry {
  def fromTry[A](f: () => Try[A],
                 maxRetries: Option[Int] = Some(DEFAULT_MAX_RETRIES),
                 backoff: Backoff = SimpleExponentialBackoff(1.second, 60.seconds, 1.1),
                 isTransient: Throwable => Boolean = transient,
                 isFatal: Throwable => Boolean = fatal
               ): A = {
    val delay = backoff.backoffMillis

    f() match {
      case Success(ret) => ret
      case Failure(e) if isFatal(e) => throw e
      case Failure(e) if !isFatal(e) =>
        val retriesLeft = if (isTransient(e)) maxRetries else maxRetries map { _ - 1 }
        if (retriesLeft.forall(_ > 0)) {
          Thread.sleep(delay)
          fromTry(f, retriesLeft, backoff, isTransient, isFatal)
        } else {
          throw e
        }
    }
  }

  def from[A](f: () => A,
                 maxRetries: Option[Int] = Some(DEFAULT_MAX_RETRIES),
                 backoff: Backoff = SimpleExponentialBackoff(1.second, 60.seconds, 1.1),
                 isTransient: Throwable => Boolean = transient,
                 isFatal: Throwable => Boolean = fatal
                ): A = {
    fromTry[A](
      () => Try{
        f()
      },
      maxRetries,
      backoff,
      isTransient,
      isFatal
    )
  }

  def transient(t: Throwable): Boolean = t match {
    case obs: ObsException if TRANSIENT_ERROR_CODES contains obs.getErrorCode => true
    case _ => false
  }

  def fatal(t: Throwable): Boolean = t match {
    case obs: ObsException if obs.getErrorCode != null && (obs.getErrorCode.startsWith("Invalid") || obs.getErrorCode.startsWith("NoSuch")) => true
    case _: ObsException => false
    case _ => true
  }

  val DEFAULT_MAX_RETRIES = 10

  val TRANSIENT_ERROR_CODES = Array("InternalError",
    "RequestTimeout",
    "RecvFlowLimitExceeded",
    "SendFlowLimitExceeded",
    "UploadTrafficRateLimitExceeded",
    "DownloadTrafficRateLimitExceeded"
  )
}
