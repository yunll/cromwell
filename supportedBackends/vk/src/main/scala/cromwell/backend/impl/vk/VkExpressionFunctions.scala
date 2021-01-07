package cromwell.backend.impl.vk

import cromwell.backend.sfs.SharedFileSystemExpressionFunctions
import cromwell.backend.standard.StandardExpressionFunctionsParams

class VkExpressionFunctions(standardParams: StandardExpressionFunctionsParams) extends SharedFileSystemExpressionFunctions(standardParams) {

  override def preMapping(str: String) = {
    if (str.startsWith("/") || str.startsWith("ftp://")) str
    else standardParams.callContext.root.resolve(str).toString
  }
}
