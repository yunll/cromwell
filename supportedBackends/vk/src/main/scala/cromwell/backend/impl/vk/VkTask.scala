package cromwell.backend.impl.vk

import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor}
import cromwell.core.path.Path
import skuber.Resource.Quantity
import skuber.Volume.{Mount, PersistentVolumeClaimRef}
import skuber.{Container, Pod, Resource, RestartPolicy, Volume}
import wdl4s.parser.MemoryUnit

final case class VkTask(jobDescriptor: BackendJobDescriptor,
                        configurationDescriptor: BackendConfigurationDescriptor,
                        vkPaths: VkJobPaths,
                        runtimeAttributes: VkRuntimeAttributes,
                        containerWorkDir: Path,
                        dockerImageUsed: String,
                        jobShell: String) {

  private val workflowDescriptor = jobDescriptor.workflowDescriptor
  private val workflowId = workflowDescriptor.id
  private val fullyQualifiedTaskName = jobDescriptor.taskCall.localName.toLowerCase()
  val name: String = fullyQualifiedTaskName + "-" + workflowId

  // contains the script to be executed
  private val commandScriptPath = vkPaths.callExecutionDockerRoot.resolve("script").toString


  private val _ :: ram :: _ = Seq(runtimeAttributes.disk, runtimeAttributes.memory) map {
    case Some(x) =>
      Option(x.to(MemoryUnit.GB).amount)
    case None =>
      None
  }

  val resources = Option(Resource.Requirements(
    requests = Map(
      "cpu" -> Quantity(runtimeAttributes.cpu.map(_.value.toString).getOrElse("0.5")),
      "memory" -> Quantity(ram.getOrElse("1Gi").toString),
    ),
    limits = Map(
      "cpu" -> Quantity(runtimeAttributes.cpu.map(_.value.toString).getOrElse("0.5")),
      "memory" -> Quantity(ram.getOrElse("1Gi").toString),
    )
  ))

  val pvc = Option(configurationDescriptor.backendConfig.getString("pvc"))
  var mountPath = Option(configurationDescriptor.backendConfig.getString("mountPath")).getOrElse(configurationDescriptor.backendConfig.getString("dockerRoot"))

  val containers = List(Container(
    name = fullyQualifiedTaskName,
    image = dockerImageUsed,
    command = List(jobShell, commandScriptPath),
    workingDir = runtimeAttributes.dockerWorkingDir,
    resources = resources,
    volumeMounts = if(!pvc.isEmpty) List(Mount(
      name = pvc.get,
      mountPath = mountPath
    )) else Nil
  ))


  val podSpec = Pod.Spec(
    containers = containers,
    volumes = if(!pvc.isEmpty) List(Volume(
      name = pvc.get,
      source = PersistentVolumeClaimRef(
        claimName = pvc.get
      )
    )) else Nil,
    restartPolicy = RestartPolicy.OnFailure,
  )

  val templateSpec = Pod.Template.Spec.named(name=fullyQualifiedTaskName).withPodSpec(podSpec)
}
