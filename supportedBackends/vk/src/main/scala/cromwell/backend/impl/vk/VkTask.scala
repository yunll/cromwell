package cromwell.backend.impl.vk

import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor}
import cromwell.core.path.Path
import skuber.Resource.Quantity
import skuber.Volume.{Mount, PersistentVolumeClaimRef}
import skuber.{Container, LocalObjectReference, Pod, Resource, RestartPolicy, Volume}
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
  private val index = jobDescriptor.key.index.getOrElse(0)
  private val attempt = jobDescriptor.key.attempt
  val name: String = fullyQualifiedTaskName + "-" + index + "-" + attempt + "-" + workflowId.shortString

  // contains the script to be executed
  val commandScriptPath = vkPaths.callExecutionDockerRoot.resolve("script").toString


  private val _ :: ram :: _ = Seq(runtimeAttributes.disk, runtimeAttributes.memory) map {
    case Some(x) =>
      Option(x.to(MemoryUnit.GB).amount)
    case None =>
      None
  }

  val resources = Option(Resource.Requirements(
    requests = Map(
      "cpu" -> Quantity(runtimeAttributes.cpu.map(_.value.toString).getOrElse((ram.getOrElse(2.0)/2).toInt.toString)),
      "memory" -> Quantity(ram.getOrElse(2).toString+"Gi"),
    ),
    limits = Map(
      "cpu" -> Quantity(runtimeAttributes.cpu.map(_.value.toString).getOrElse((ram.getOrElse(2.0)/2).toInt.toString)),
      "memory" -> Quantity(ram.getOrElse(2).toString+"Gi"),
    )
  ))

  val pvc = if (configurationDescriptor.backendConfig.hasPath("pvc")){
    configurationDescriptor.backendConfig.getString("pvc")
  }else{
    ""
  }
  val mountPath = if (configurationDescriptor.backendConfig.hasPath("dockerRoot")){
    configurationDescriptor.backendConfig.getString("dockerRoot")
  }else{
    configurationDescriptor.backendConfig.getString("root")
  }

  val containers = List(Container(
    name = fullyQualifiedTaskName,
    image = dockerImageUsed,
    command = List(jobShell, commandScriptPath),
    workingDir = runtimeAttributes.dockerWorkingDir,
    resources = resources,
    volumeMounts = if(!pvc.equals("")) List(Mount(
      name = pvc,
      mountPath = mountPath
    )) else Nil
  ))


  val podSpec = Pod.Spec(
    containers = containers,
    volumes = if(!pvc.equals("")) List(Volume(
      name = pvc,
      source = PersistentVolumeClaimRef(
        claimName = pvc
      )
    )) else Nil,
    restartPolicy = RestartPolicy.OnFailure,
    imagePullSecrets = List(LocalObjectReference(
      name = "imagepull-secret"
    ))
  )

  val templateSpec = Pod.Template.Spec.named(name=fullyQualifiedTaskName).withPodSpec(podSpec)
}