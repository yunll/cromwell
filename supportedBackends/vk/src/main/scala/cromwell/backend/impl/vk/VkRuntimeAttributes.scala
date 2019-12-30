package cromwell.backend.impl.vk

import cats.data.Validated._
import cats.instances.list._
import cats.syntax.traverse._
import cats.syntax.validated._
import com.typesafe.config.Config
import common.validation.ErrorOr.ErrorOr
import cromwell.backend.standard.StandardValidatedRuntimeAttributesBuilder
import cromwell.backend.validation.{MaxRetriesValidation, _}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import wom.RuntimeAttributesKeys
import wom.format.MemorySize
import wom.types.{WomArrayType, WomStringType, WomType}
import wom.values._

case class VkRuntimeAttributes(continueOnReturnCode: ContinueOnReturnCode,
                               dockerImage: String,
                               dockerWorkingDir: Option[String],
                               failOnStderr: Boolean,
                               cpu: Option[Double],
                               memory: Option[MemorySize],
                               disks: Option[Seq[VkAttachedDisk]],
                               maxRetries: Option[Int],
                               gpuCount: Option[Int Refined Positive],
                               gpuType: Option[String])

object VkRuntimeAttributes {

  val DockerWorkingDirKey = "dockerWorkingDir"
  val DisksKey = "disks"

  private def cpuValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[Double] = new FloatRuntimeAttributesValidation(RuntimeAttributesKeys.CpuKey).optional

  private def gpuCountValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[Int Refined Positive] = new PositiveIntRuntimeAttributesValidation(RuntimeAttributesKeys.GpuKey).optional

  private def gpuTypeValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[String] = new StringRuntimeAttributesValidation(RuntimeAttributesKeys.GpuTypeKey).optional

  private def failOnStderrValidation(runtimeConfig: Option[Config]) : RuntimeAttributesValidation[Boolean] = FailOnStderrValidation.default(runtimeConfig)

  private def continueOnReturnCodeValidation(runtimeConfig: Option[Config]) : RuntimeAttributesValidation[ContinueOnReturnCode] = ContinueOnReturnCodeValidation.default(runtimeConfig)

  private def disksValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[Seq[VkAttachedDisk]] = DisksValidation.optional

  private def memoryValidation(runtimeConfig: Option[Config]): OptionalRuntimeAttributesValidation[MemorySize] = MemoryValidation.optional(RuntimeAttributesKeys.MemoryKey)

  private val dockerValidation: RuntimeAttributesValidation[String] = DockerValidation.instance

  private val maxRetriesValidation: OptionalRuntimeAttributesValidation[Int] = MaxRetriesValidation.optional

  private val dockerWorkingDirValidation: OptionalRuntimeAttributesValidation[String] = DockerWorkingDirValidation.optional

  def runtimeAttributesBuilder(backendRuntimeConfig: Option[Config]): StandardValidatedRuntimeAttributesBuilder =
    StandardValidatedRuntimeAttributesBuilder.default(backendRuntimeConfig).withValidation(
      cpuValidation(backendRuntimeConfig),
      memoryValidation(backendRuntimeConfig),
      disksValidation(backendRuntimeConfig),
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
    val disks: Option[Seq[VkAttachedDisk]]= RuntimeAttributesValidation.extractOption(disksValidation(backendRuntimeConfig).key, validatedRuntimeAttributes)
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
      disks,
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

object DisksValidation extends RuntimeAttributesValidation[Seq[VkAttachedDisk]] {
  override def key: String = VkRuntimeAttributes.DisksKey

  override def coercion: Traversable[WomType] = Set(WomStringType, WomArrayType(WomStringType))

  override protected def validateValue: PartialFunction[WomValue, ErrorOr[Seq[VkAttachedDisk]]] = {
    case WomString(value) => validateLocalDisks(value.split(",\\s*").toSeq)
    case WomArray(womType, values) if womType.memberType == WomStringType =>
      validateLocalDisks(values.map(_.valueString))
  }

  private def validateLocalDisks(disks: Seq[String]): ErrorOr[Seq[VkAttachedDisk]] = {
    val diskNels: ErrorOr[Seq[VkAttachedDisk]] = disks.toList.traverse[ErrorOr, VkAttachedDisk](validateLocalDisk)
    diskNels
  }

  private def validateLocalDisk(disk: String): ErrorOr[VkAttachedDisk] = {
    VkAttachedDisk.parse(disk) match {
      case scala.util.Success(attachedDisk) => attachedDisk.validNel
      case scala.util.Failure(ex) => ex.getMessage.invalidNel
    }
  }

  override protected def missingValueMessage: String =
    s"Expecting $key runtime attribute to be a comma separated String or Array[String]"
}
