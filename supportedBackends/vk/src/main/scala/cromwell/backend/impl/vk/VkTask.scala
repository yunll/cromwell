package cromwell.backend.impl.vk

import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor}
import cromwell.core.path.Path
import skuber.Resource.Quantity
import skuber.Volume.{Mount, PersistentVolumeClaimRef}
import skuber.batch.Job
import skuber.{Container, LocalObjectReference, ObjectMeta, Pod, Resource, RestartPolicy, Volume}
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

  val cpu = runtimeAttributes.cpu.map(_.value.toString).getOrElse((ram.getOrElse(2.0)/2).toInt.toString)
  val resources = if(runtimeAttributes.gpuType.isEmpty && runtimeAttributes.gpuType.isEmpty){
    Option(Resource.Requirements(
      requests = Map(
        "cpu" -> Quantity(cpu),
        "memory" -> Quantity(ram.getOrElse(2).toString+"Gi"),
      ),
      limits = Map(
        "cpu" -> Quantity(cpu),
        "memory" -> Quantity(ram.getOrElse(2).toString+"Gi"),
      )
    ))
  } else {
    val gpu = runtimeAttributes.gpuCount.map(_.value.toString).get
    val gpuType = runtimeAttributes.gpuType.get
    Option(Resource.Requirements(
      requests = Map(
        "cpu" -> Quantity(cpu),
        "memory" -> Quantity(ram.getOrElse(2).toString+"Gi"),
        gpuType -> Quantity(gpu),
      ),
      limits = Map(
        "cpu" -> Quantity(cpu),
        "memory" -> Quantity(ram.getOrElse(2).toString+"Gi"),
        gpuType -> Quantity(gpu),
      )
    ))
  }

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

  val labels = Map(
    "operator-id" -> workflowId.toString
  )

  val podMetadata = ObjectMeta(name=fullyQualifiedTaskName,labels = labels)

  val templateSpec = Pod.Template.Spec(metadata=podMetadata).withPodSpec(podSpec)

  val jobMetadata = ObjectMeta(name=name,labels = labels)

  val job = Job(metadata=jobMetadata).withTemplate(templateSpec)
}
