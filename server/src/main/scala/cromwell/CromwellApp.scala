package cromwell

//import cromwell.core.path.DefaultPathBuilder

//import scala.util.Properties



object CromwellApp extends App {

  sealed trait Command
  case object Run extends Command
  case object Server extends Command
  case object Submit extends Command
  
  def buildParser(): scopt.OptionParser[CommandLineArguments] = new CommandLineParser()

  def runCromwell(args: CommandLineArguments): Unit = {
    args.command match {
      case Some(Run) => CromwellEntryPoint.runSingle(args)
      case Some(Server) => CromwellEntryPoint.runServer()
      case Some(Submit) => CromwellEntryPoint.submitToServer(args)
      case None => parser.showUsage()
    }
  }

  val parser = buildParser()
  val parsedArgs = parser.parse(args, CommandLineArguments())
//  Properties.setProp("config.file", "/Users/dts/cromwell/cromwell/vk-obs.conf")
//  val parsedArgs = parser.parse(args, CommandLineArguments(command = Option(Run),workflowSource = Option("/Users/dts/cromwell/cromwell/chain.wdl"),workflowInputs = Option(DefaultPathBuilder.build("/Users/dts/cromwell/cromwell/chain.inputs").get)))
  parsedArgs match {
    case Some(pa) => runCromwell(pa)
    case None => parser.showUsage()
  }
}
