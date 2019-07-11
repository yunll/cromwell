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
    validate { if(instanceConfig.hasPath("auth.endpoint")){
      instanceConfig.as[String]("auth.endpoint")
    } else {
      s"obs.${globalConfig.as[String]("backend.providers.VK.config.region")}.myhuaweicloud.com"
    } },
    validate { if(instanceConfig.hasPath("auth.accessKey")){
      instanceConfig.as[String]("auth.accessKey")
    } else {
      globalConfig.as[String]("backend.providers.VK.config.accessKey")
    } },
    validate { if(instanceConfig.hasPath("auth.secretKey")){
      instanceConfig.as[String]("auth.accessKey")
    } else {
      globalConfig.as[String]("backend.providers.VK.config.secretKey")
    } }
  ).tupled.unsafe("OBS filesystem configuration is invalid")
  
  def withOptions(options: WorkflowOptions)(implicit as: ActorSystem, ec: ExecutionContext) = {
    Future.successful(ObsPathBuilder.fromConfiguration(endpoint, accessKey, securityKey, options))
  }
}
