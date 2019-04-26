package cromwell.core

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{ActorInitializationException, OneForOneStrategy, SupervisorStrategy, SupervisorStrategyConfigurator}
import org.slf4j.LoggerFactory

class CromwellUserGuardianStrategy extends SupervisorStrategyConfigurator {
  val log = LoggerFactory.getLogger(classOf[CromwellUserGuardianStrategy])
  override def create(): SupervisorStrategy = OneForOneStrategy() {
    case _: ActorInitializationException => Escalate
    case e: Exception =>
      log.error("Actor terminating due to uncaught exception, initiating restart", e)
      Restart
    case t =>
      SupervisorStrategy.defaultDecider.applyOrElse(t, (_: Any) => Escalate)
  }
}
