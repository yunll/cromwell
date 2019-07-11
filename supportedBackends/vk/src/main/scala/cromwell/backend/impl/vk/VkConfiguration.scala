package cromwell.backend.impl.vk

import cromwell.backend.BackendConfigurationDescriptor

class VkConfiguration(val configurationDescriptor: BackendConfigurationDescriptor) {
  val namespace = configurationDescriptor.backendConfig.getString("namespace")
  val storagePath = Option(configurationDescriptor.backendConfig.getString("storagePath"))
  val region = Option(configurationDescriptor.backendConfig.getString("region")).getOrElse("cn-north-4")
  val accessKey = configurationDescriptor.backendConfig.getString("accessKey")
  val secretKey = configurationDescriptor.backendConfig.getString("secretKey")
  val runtimeConfig = configurationDescriptor.backendRuntimeAttributesConfig
}
