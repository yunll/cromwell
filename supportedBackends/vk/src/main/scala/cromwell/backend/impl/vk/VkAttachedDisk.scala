package cromwell.backend.impl.vk

import cats.data.Validated._
import cats.syntax.apply._
import cats.syntax.validated._
import common.exception.MessageAggregation
import common.validation.ErrorOr._
import cromwell.backend.DiskPatterns._
import cromwell.core.path.{DefaultPathBuilder, Path}
import wdl4s.parser.MemoryUnit
import wom.format.MemorySize

import scala.util.Try

object VkAttachedDisk {
  def parse(s: String): Try[VkAttachedDisk] = {

    def sizeGbValidation(sizeGbString: String): ErrorOr[Int] = validateLong(sizeGbString).map(_.toInt)
    def diskTypeValidation(diskTypeString: String): ErrorOr[VkDiskType] = validateDiskType(diskTypeString)

    val validation: ErrorOr[VkAttachedDisk] = s match {
      case WorkingDiskPattern(sizeGb, diskType) => (validateDiskType(diskType), sizeGbValidation(sizeGb)) mapN { VkWorkingDisk.apply }
      case MountedDiskPattern(mountPoint, sizeGb, diskType) => (sizeGbValidation(sizeGb), diskTypeValidation(diskType)) mapN { (s, dt) => VkEmptyMountedDisk(dt, s, DefaultPathBuilder.get(mountPoint)) }
      case _ => s"Disk strings should be of the format 'local-disk SIZE TYPE' or '/mount/point SIZE TYPE' but got: '$s'".invalidNel
    }

    Try(validation match {
      case Valid(localDisk) => localDisk
      case Invalid(nels) =>
        throw new UnsupportedOperationException with MessageAggregation {
          val exceptionContext = ""
          val errorMessages: List[String] = nels.toList
        }
    })
  }

  private def validateDiskType(diskTypeName: String): ErrorOr[VkDiskType] = {
    VkDiskType.values().find(_.diskTypeName == diskTypeName) match {
      case Some(diskType) => diskType.validNel
      case None =>
        val diskTypeNames = VkDiskType.values.map(_.diskTypeName).mkString(", ")
        s"Disk TYPE $diskTypeName should be one of $diskTypeNames".invalidNel
    }
  }

  private def validateLong(value: String): ErrorOr[Long] = {
    try {
      value.toLong.validNel
    } catch {
      case _: IllegalArgumentException => s"$value not convertible to a Long".invalidNel
    }
  }

  implicit class EnhancedDisks(val disks: Seq[VkAttachedDisk]) extends AnyVal {
    def adjustWorkingDiskWithNewMin(minimum: MemorySize, onAdjustment: => Unit): Seq[VkAttachedDisk] = disks map {
      case disk: VkWorkingDisk if disk == VkWorkingDisk.Default && disk.sizeGb < minimum.to(MemoryUnit.GB).amount.toInt =>
        onAdjustment
        disk.copy(sizeGb = minimum.to(MemoryUnit.GB).amount.toInt)
      case other => other
    }
  }
}

trait VkAttachedDisk {
  def name: String
  def diskType: VkDiskType
  def sizeGb: Int
  def mountPoint: Path
}

case class VkEmptyMountedDisk(diskType: VkDiskType, sizeGb: Int, mountPoint: Path) extends VkAttachedDisk {
  val name = s"d-${BigInt.probablePrime(100,scala.util.Random).toString(32)}"
  override def toString: String = s"$mountPoint $sizeGb ${diskType.diskTypeName}"
}

object VkWorkingDisk {
  val MountPoint: Path = DefaultPathBuilder.get("/cromwell_root")
  val Name = "local-disk"
  val Default = VkWorkingDisk(VkDiskType.SSD, 100)
}

case class VkWorkingDisk(diskType: VkDiskType, sizeGb: Int) extends VkAttachedDisk {
  val mountPoint = VkWorkingDisk.MountPoint
  val name = VkWorkingDisk.Name
  override def toString: String = s"$name $sizeGb ${diskType.diskTypeName}"
}
