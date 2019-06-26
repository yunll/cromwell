package cromwell.backend.impl.vk

/**
  * GRANULAR: The VK payload will list outputs individually in the outputs section of the payload
  * ROOT: The VK payload will list only the root execution directory in the outputs section of the payload
  */
object OutputMode extends Enumeration {
  type OutputMode = Value
  val GRANULAR, ROOT = Value
}
