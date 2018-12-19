package womtool.graph

import java.util.concurrent.atomic.AtomicInteger

import cats.implicits._
import wom.graph._

object GraphPrint {

  final case class WorkflowDigraph(workflowName: String, digraph: NodesAndLinks)
  final case class NodesAndLinks(nodes: Set[String], links: Set[String])
  implicit val monoid = cats.derived.MkMonoid[NodesAndLinks]

  def generateWorkflowDigraph(graph: Graph): WorkflowDigraph = {

    val digraph = listAllGraphNodes(graph)

    WorkflowDigraph("workflow", digraph)
  }

  private val clusterCount: AtomicInteger = new AtomicInteger(0)

  final case class SubGraph(root: GraphNode, subGraph: Graph, name: String)


  private def listAllGraphNodes(graph: Graph): NodesAndLinks = {

    val callsAndDeclarations: Set[GraphNode] = (graph.nodes collect {
      case w: GraphNode if isCallOrCallBasedDeclaration(w) => w
    }).toSet

    val subGraphs: Set[SubGraph] = (graph.nodes collect {
      case scatter: ScatterNode =>
        val expression =
          scatter.scatterVariableNodes.map(v => s"${v.identifier.localName.value} in ${v.scatterExpressionNode.inputPorts.head.name}").mkString(", ")
        SubGraph(scatter, scatter.innerGraph,
          s"scatter ($expression)")
      case conditional: ConditionalNode =>
        SubGraph(conditional, conditional.innerGraph,
          s"if (${conditional.conditionExpression.womExpression.sourceString})")
      case call: WorkflowCallNode =>
        SubGraph(call, call.callable.innerGraph, s"call ${call.callable.name}")
    }).toSet

    def upstreamLinks(graphNode: GraphNode, graphNodeName: String, suffix: String = ""): Set[String] = graphNode.upstream collect {
      case upstream: GraphNode if isCallOrCallBasedDeclaration(upstream) =>
        val upstreamName = graphName(upstream)
        s""""$upstreamName" -> "$graphNodeName" $suffix"""
    }

    val thisLevelNodesAndLinks: NodesAndLinks = callsAndDeclarations.toList foldMap { graphNode =>
      val name = graphName(graphNode)
      val initialSet: Set[String] = graphNode match {
        case w: GraphNode if isCallOrCallBasedDeclaration(w) => Set(s""""$name"""")
        case _ => Set.empty
      }

      NodesAndLinks(initialSet, upstreamLinks(graphNode, name))
    }

    val subGraphNodesAndLinks: NodesAndLinks = subGraphs.toList foldMap { subGraph =>
      val clusterName = "cluster_" + clusterCount.getAndIncrement()
      val subGraphName = subGraph.name
      val subNodes = listAllGraphNodes(subGraph.subGraph)
      val scope = s"""
         |subgraph $clusterName {
         |  ${subNodes.nodes.mkString(sep="\n  ")}
         |  "$subGraphName" [shape=plaintext]
         |}
      """.stripMargin

      NodesAndLinks(Set(scope), subNodes.links ++ upstreamLinks(subGraph.root, subGraphName, s"[lhead=$clusterName]"))
    }

    thisLevelNodesAndLinks |+| subGraphNodesAndLinks
  }

  private def isCallOrCallBasedDeclaration(w: GraphNode): Boolean = w match {
    case _: CallNode => true
    case _: GraphOutputNode => true
//    case w: Declaration if w.upstream.exists(isCallOrCallBasedDeclaration) => true
    case _ => false
  }

  private def dotSafe(s: String) = s.replaceAllLiterally("\"", "\\\"")

  private def graphName(g: GraphNode): String = dotSafe(g match {
//    case d: Declaration =>
//      s"${d.womType.toDisplayString} ${d.unqualifiedName}"
    case c: CallNode =>
      s"call ${c.callable.name}"
    case i: ConditionalNode =>
      s"if (${i.conditionExpression.womExpression.sourceString})"
    case s: ScatterNode =>
      s"scatter (${s.scatterProcessingFunction})"
    case c: GraphOutputNode =>
      s"output { ${c.graphOutputPort.identifier.localName.value} }"
    case other => s"${other.getClass.getSimpleName}: ${other.fullyQualifiedName}"
  })
}
