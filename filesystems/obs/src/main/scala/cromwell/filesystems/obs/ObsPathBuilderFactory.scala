package cromwell.filesystems.obs

import akka.actor.ActorSystem
import cats.syntax.apply._
import com.typesafe.config.Config
import common.validation.Validation._
import cromwell.core.WorkflowOptions
import cromwell.core.path.PathBuilderFactory
import net.ceedubs.ficus.Ficus._

import scala.concurrent.{ExecutionContext, Future}

final case class ObsPathBuilderFactory(globalConfig: Config, instanceConfig: Config) extends PathBuilderFactory {
  val (endpoint, accessKey, securityKey) = (
    validate { instanceConfig.as[String]("auth.endpoint") },
    validate { instanceConfig.as[String]("auth.access-key") },
    validate { instanceConfig.as[String]("auth.security-key") }
  ).tupled.unsafe("OBS filesystem configuration is invalid")
  
  def withOptions(options: WorkflowOptions)(implicit as: ActorSystem, ec: ExecutionContext) = {
    Future.successful(ObsPathBuilder.fromConfiguration(endpoint, accessKey, securityKey, options))
  }
}
