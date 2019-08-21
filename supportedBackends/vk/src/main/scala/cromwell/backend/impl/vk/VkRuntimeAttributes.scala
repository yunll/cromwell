package cromwell.backend.impl.vk

import cats.syntax.validated._
import com.typesafe.config.Config
import common.validation.ErrorOr.ErrorOr
import cromwell.backend.standard.StandardValidatedRuntimeAttributesBuilder
import cromwell.backend.validation.{MaxRetriesValidation, _}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import wom.RuntimeAttributesKeys
import wom.format.MemorySize
import wom.values._

case class VkRuntimeAttributes(continueOnReturnCode: ContinueOnReturnCode,
                               dockerImage: String,
                               dockerWorkingDir: Option[String],
                               failOnStderr: Boolean,
                               cpu: Option[Double],
                               memory: Option[MemorySize],
                               disk: Option[MemorySize],
                               maxRetries: Option[Int],
                               gpuCount: Option[Int Refined Positive],
                               gpuType: Option[String])

object VkRuntimeAttributes {

  val DockerWorkingDirKey = "dockerWorkingDir"
  val DiskSizeKey = "disk"

  private def cpuValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[Double] = new FloatRuntimeAttributesValidation("cpu").optional

  private def gpuCountValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[Int Refined Positive] = new PositiveIntRuntimeAttributesValidation("gpuCount").optional

  private def gpuTypeValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[String] = new StringRuntimeAttributesValidation("gpuType").optional

  private def failOnStderrValidation(runtimeConfig: Option[Config]) = FailOnStderrValidation.default(runtimeConfig)

  private def continueOnReturnCodeValidation(runtimeConfig: Option[Config]) = ContinueOnReturnCodeValidation.default(runtimeConfig)

  private def diskSizeValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[MemorySize] = MemoryValidation.optional(DiskSizeKey)

  private def memoryValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[MemorySize] = MemoryValidation.optional(RuntimeAttributesKeys.MemoryKey)

  private val dockerValidation: RuntimeAttributesValidation[String] = DockerValidation.instance

  private val maxRetriesValidation: OptionalRuntimeAttributesValidation[Int] = MaxRetriesValidation.optional

  private val dockerWorkingDirValidation: OptionalRuntimeAttributesValidation[String] = DockerWorkingDirValidation.optional

  def runtimeAttributesBuilder(backendRuntimeConfig: Option[Config]): StandardValidatedRuntimeAttributesBuilder =
    StandardValidatedRuntimeAttributesBuilder.default(backendRuntimeConfig).withValidation(
      cpuValidation(backendRuntimeConfig),
      memoryValidation(backendRuntimeConfig),
      diskSizeValidation(backendRuntimeConfig),
      maxRetriesValidation,
      dockerValidation,
      dockerWorkingDirValidation,
      gpuCountValidation(backendRuntimeConfig),
      gpuTypeValidation(backendRuntimeConfig),
    )

  def apply(validatedRuntimeAttributes: ValidatedRuntimeAttributes, backendRuntimeConfig: Option[Config]): VkRuntimeAttributes = {
    val docker: String = RuntimeAttributesValidation.extract(dockerValidation, validatedRuntimeAttributes)
    val dockerWorkingDir: Option[String] = RuntimeAttributesValidation.extractOption(dockerWorkingDirValidation.key, validatedRuntimeAttributes)
    val cpu: Option[Double] = RuntimeAttributesValidation.extractOption(cpuValidation(backendRuntimeConfig).key, validatedRuntimeAttributes)
    val gpuCount: Option[Int Refined Positive] = RuntimeAttributesValidation.extractOption(gpuCountValidation(backendRuntimeConfig).key, validatedRuntimeAttributes)
    val gpuType: Option[String] = RuntimeAttributesValidation.extractOption(gpuTypeValidation(backendRuntimeConfig).key, validatedRuntimeAttributes)
    val memory: Option[MemorySize] = RuntimeAttributesValidation.extractOption(memoryValidation(backendRuntimeConfig).key, validatedRuntimeAttributes)
    val disk: Option[MemorySize] = RuntimeAttributesValidation.extractOption(diskSizeValidation(backendRuntimeConfig).key, validatedRuntimeAttributes)
    val failOnStderr: Boolean =
      RuntimeAttributesValidation.extract(failOnStderrValidation(backendRuntimeConfig), validatedRuntimeAttributes)
    val continueOnReturnCode: ContinueOnReturnCode =
      RuntimeAttributesValidation.extract(continueOnReturnCodeValidation(backendRuntimeConfig), validatedRuntimeAttributes)
    val maxRetries: Option[Int] = RuntimeAttributesValidation.extractOption(maxRetriesValidation.key, validatedRuntimeAttributes)
    new VkRuntimeAttributes(
      continueOnReturnCode,
      docker,
      dockerWorkingDir,
      failOnStderr,
      cpu,
      memory,
      disk,
      maxRetries,
      gpuCount,
      gpuType
    )
  }
}

object DockerWorkingDirValidation {
  lazy val instance: RuntimeAttributesValidation[String] = new DockerWorkingDirValidation
  lazy val optional: OptionalRuntimeAttributesValidation[String] = instance.optional
}

class DockerWorkingDirValidation extends StringRuntimeAttributesValidation(VkRuntimeAttributes.DockerWorkingDirKey) {
  // NOTE: Docker's current test specs don't like WdlInteger, etc. auto converted to WdlString.
  override protected def validateValue: PartialFunction[WomValue, ErrorOr[String]] = {
    case WomString(value) => value.validNel
  }
}

