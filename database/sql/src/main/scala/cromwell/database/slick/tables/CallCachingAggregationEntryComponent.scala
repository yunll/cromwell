package cromwell.database.slick.tables

import cromwell.database.sql.tables.CallCachingAggregationEntry


trait CallCachingAggregationEntryComponent {

  this: DriverComponent with CallCachingEntryComponent with CallCachingDetritusEntryComponent =>

  import driver.api._

  class CallCachingAggregationEntries(tag: Tag) extends Table[CallCachingAggregationEntry](tag, "CALL_CACHING_AGGREGATION_ENTRY") {
    def callCachingAggregationEntryId = column[Int]("CALL_CACHING_AGGREGATION_ENTRY_ID", O.PrimaryKey, O.AutoInc)

    def baseAggregation = column[String]("BASE_AGGREGATION", O.Length(255))

    def inputFilesAggregation = column[Option[String]]("INPUT_FILES_AGGREGATION", O.Length(255))

    def callCachingEntryId = column[Int]("CALL_CACHING_ENTRY_ID")

    override def * = (baseAggregation, inputFilesAggregation, callCachingEntryId.?, callCachingAggregationEntryId.?) <>
      (CallCachingAggregationEntry.tupled, CallCachingAggregationEntry.unapply)

    def fkCallCachingAggregationEntryCallCachingEntryId = foreignKey("FK_CALL_CACHING_AGGREGATION_ENTRY_CALL_CACHING_ENTRY_ID",
      callCachingEntryId, callCachingEntries)(_.callCachingEntryId)

    def ixCallCachingAggregationEntryBaIfa =
      index("IX_CALL_CACHING_AGGREGATION_ENTRY_BA_IFA", (baseAggregation, inputFilesAggregation), unique = false)
  }

  val callCachingAggregationEntries = TableQuery[CallCachingAggregationEntries]

  val callCachingAggregationEntryIdsAutoInc = callCachingAggregationEntries returning
    callCachingAggregationEntries.map(_.callCachingAggregationEntryId)

  val callCachingAggregationForCacheEntryId = Compiled(
    (callCachingEntryId: Rep[Int]) => for {
      callCachingAggregationEntry <- callCachingAggregationEntries
      if callCachingAggregationEntry.callCachingEntryId === callCachingEntryId
    } yield callCachingAggregationEntry
  )

  val existsCallCachingEntriesForBaseAggregationHash = Compiled(
    (baseAggregation: Rep[String]) => (for {
      callCachingEntry <- callCachingEntries
      if callCachingEntry.allowResultReuse
      callCachingAggregationEntry <- callCachingAggregationEntries
      if callCachingEntry.callCachingEntryId === callCachingAggregationEntry.callCachingEntryId
      if callCachingAggregationEntry.baseAggregation === baseAggregation
    } yield ()).exists
  )

  val existsCallCachingEntriesForBaseAggregationHashWithCallCachePrefix = Compiled(
    (baseAggregation: Rep[String],
     prefix1: Rep[String], prefix1Length: Rep[Int],
     prefix2: Rep[String], prefix2Length: Rep[Int],
     prefix3: Rep[String], prefix3Length: Rep[Int]
    ) => (for {
      callCachingEntry <- callCachingEntries
      if callCachingEntry.allowResultReuse
      callCachingAggregationEntry <- callCachingAggregationEntries
      if callCachingEntry.callCachingEntryId === callCachingAggregationEntry.callCachingEntryId
      if callCachingAggregationEntry.baseAggregation === baseAggregation
      detritus <- callCachingDetritusEntries
      if detritus.callCachingEntryId === callCachingEntry.callCachingEntryId
      detritusPath = detritus.detritusValue.map { x => x.asColumnOf[String] }
      if (detritusPath.substring(0, prefix1Length) === prefix1) ||
        (detritusPath.substring(0, prefix2Length) === prefix2) ||
        (detritusPath.substring(0, prefix3Length) === prefix3)} yield ()).exists
  )

//  def existsCallCachingEntriesForBaseAggregationHashWithCallCachePrefixList(baseAggregation: Rep[String], prefixList: List[String]): Rep[Boolean] = {
//    //val cnt: Rep[Int] = (10)
//
//    (for {
//        callCachingEntry <- callCachingEntries
//        if callCachingEntry.allowResultReuse
//        callCachingAggregationEntry <- callCachingAggregationEntries
//        if callCachingEntry.callCachingEntryId === callCachingAggregationEntry.callCachingEntryId
//        if callCachingAggregationEntry.baseAggregation === baseAggregation
//        detritus <- callCachingDetritusEntries
//        if detritus.callCachingEntryId === callCachingEntry.callCachingEntryId
//        detritusPath = detritus.detritusValue.map { x => x.asColumnOf[String] }
////        if prefixList.map(prefix => detritusPath.substring(0, prefix.length) === prefix).reduce(_ || _)
//        //if detritusPath.take(cnt) inSet prefixList
//        if detritusPath.take(detritusPath.drop(5).indexOf("/").getOrElse(0)) inSet prefixList
//      } yield ()).exists
//  }

  def existsCallCachingEntriesForBaseAggregationHashWithCallCachePrefixList(baseAggregation: String, prefixList: List[String]) = {
    val repeatedGeneratedSql = prefixList.map(s =>
      s"(({fn substring(x6.x5, 1, ${s.length})}) = '$s')").mkString(" or \n")

    val str =
      s"""
        |select exists(select 1
        |              from `CALL_CACHING_ENTRY` x2,
        |                   `CALL_CACHING_AGGREGATION_ENTRY` x3,
        |                   (select `CALL_CACHING_ENTRY_ID`                               as x4,
        |                           `DETRITUS_VALUE` as x5
        |                    from `CALL_CACHING_DETRITUS_ENTRY`) x6
        |              where ((x3.`BASE_AGGREGATION` = '$baseAggregation') and
        |                     (
        |
        |
        |                     (
        |                       $repeatedGeneratedSql
        |)
        |
        |                    )
        |                and ((x2.`ALLOW_RESULT_REUSE` and (x2.`CALL_CACHING_ENTRY_ID` = x3.`CALL_CACHING_ENTRY_ID`)) and
        |                     (x6.x4 = x2.`CALL_CACHING_ENTRY_ID`))))
      """.stripMargin

//    println("----------------------")
//    println(str)

    sql"""
         #$str
       """.stripMargin.as[Boolean].head
  }

  def callCachingEntriesForAggregatedHashes(baseAggregation: Rep[String], inputFilesAggregation: Rep[Option[String]], number: Int) = {
    (for {
      callCachingEntry <- callCachingEntries
      if callCachingEntry.allowResultReuse
      callCachingAggregationEntry <- callCachingAggregationEntries
      if callCachingEntry.callCachingEntryId === callCachingAggregationEntry.callCachingEntryId
      if callCachingAggregationEntry.baseAggregation === baseAggregation
      if (callCachingAggregationEntry.inputFilesAggregation.isEmpty && inputFilesAggregation.isEmpty) ||
        (callCachingAggregationEntry.inputFilesAggregation === inputFilesAggregation)
    } yield callCachingAggregationEntry.callCachingEntryId).drop(number - 1).take(1)
  }

  def callCachingEntriesForAggregatedHashesWithPrefixes(baseAggregation: Rep[String], inputFilesAggregation: Rep[Option[String]],
                                                        prefix1: Rep[String], prefix1Length: Rep[Int],
                                                        prefix2: Rep[String], prefix2Length: Rep[Int],
                                                        prefix3: Rep[String], prefix3Length: Rep[Int],
                                                        number: Int) = {
    (for {
      callCachingEntry <- callCachingEntries
      if callCachingEntry.allowResultReuse
      callCachingAggregationEntry <- callCachingAggregationEntries
      if callCachingEntry.callCachingEntryId === callCachingAggregationEntry.callCachingEntryId
      if callCachingAggregationEntry.baseAggregation === baseAggregation
      if (callCachingAggregationEntry.inputFilesAggregation.isEmpty && inputFilesAggregation.isEmpty) ||
        (callCachingAggregationEntry.inputFilesAggregation === inputFilesAggregation)
      detritus <- callCachingDetritusEntries
      // Pick only one detritus file since this is not an existence check and we don't want to return one row
      // for each of the (currently 6) types of standard detritus.
      if detritus.detritusKey === "returnCode"
      if detritus.callCachingEntryId === callCachingEntry.callCachingEntryId
      detritusPath = detritus.detritusValue.map { x => x.asColumnOf[String] }
      if (detritusPath.substring(0, prefix1Length) === prefix1) ||
        (detritusPath.substring(0, prefix2Length) === prefix2) ||
        (detritusPath.substring(0, prefix3Length) === prefix3)
    } yield callCachingAggregationEntry.callCachingEntryId).drop(number - 1).take(1)
  }

  def callCachingEntriesForAggregatedHashesWithPrefixesList(baseAggregation: Rep[String], inputFilesAggregation: Rep[Option[String]],
                                                            prefixList: List[String], number: Int): Query[Rep[Int], Int, Seq] = {
    (for {
      callCachingEntry <- callCachingEntries
      if callCachingEntry.allowResultReuse
      callCachingAggregationEntry <- callCachingAggregationEntries
      if callCachingEntry.callCachingEntryId === callCachingAggregationEntry.callCachingEntryId
      if callCachingAggregationEntry.baseAggregation === baseAggregation
      if (callCachingAggregationEntry.inputFilesAggregation.isEmpty && inputFilesAggregation.isEmpty) ||
        (callCachingAggregationEntry.inputFilesAggregation === inputFilesAggregation)
      detritus <- callCachingDetritusEntries
      // Pick only one detritus file since this is not an existence check and we don't want to return one row
      // for each of the (currently 6) types of standard detritus.
      if detritus.detritusKey === "returnCode"
      if detritus.callCachingEntryId === callCachingEntry.callCachingEntryId
      detritusPath = detritus.detritusValue.map { x => x.asColumnOf[String] }
      if prefixList.map(prefix => detritusPath.substring(0, prefix.length) === prefix).reduce(_ || _)
    } yield callCachingAggregationEntry.callCachingEntryId).drop(number - 1).take(1)
  }

  def callCachingEntriesForAggregatedHashesWithPrefixesList(baseAggregation: String, inputFilesAggregation: Option[String],
                                                            prefixList: List[String], number: Int) = {
    val repeatedGeneratedSql = prefixList.map(s =>
      s"(({fn substring(x7.x6, 1, ${s.length})}) = '$s')").mkString(" or \n")

    val inputs = inputFilesAggregation.getOrElse("null")

    val start = number - 1

    val str =
      s"""
         |select
         |   x2.`CALL_CACHING_ENTRY_ID`
         |from
         |   `CALL_CACHING_ENTRY` x3,
         |   `CALL_CACHING_AGGREGATION_ENTRY` x2,
         |   (
         |      select
         |         `DETRITUS_KEY` as x4,
         |         `CALL_CACHING_ENTRY_ID` as x5,
         |         `DETRITUS_VALUE` as x6
         |      from
         |         `CALL_CACHING_DETRITUS_ENTRY`
         |   )
         |   x7
         |where
         |   ((((x2.`BASE_AGGREGATION` = '$baseAggregation')
         |      and
         |      (((x2.`INPUT_FILES_AGGREGATION` is null)
         |         and
         |         (
         |            '$inputs' is null
         |         )
         |)
         |         or
         |         (
         |            x2.`INPUT_FILES_AGGREGATION` = '$inputs'
         |         )
         |      )
         |)
         |      and
         |      (
         |         x7.x4 = 'returnCode'
         |      )
         |)
         |      and
         |      (
         |         (
         |            $repeatedGeneratedSql
         |      )
         |   )
         |   and
         |   (
         |(x3.`ALLOW_RESULT_REUSE`
         |      and
         |      (
         |         x3.`CALL_CACHING_ENTRY_ID` = x2.`CALL_CACHING_ENTRY_ID`
         |      )
         |)
         |      and
         |      (
         |         x7.x5 = x3.`CALL_CACHING_ENTRY_ID`
         |      )
         |   ))
         |   limit $start, 1;
      """.stripMargin

    //    println("----------------------")
    //    println(str)

    sql"""
         #$str
       """.stripMargin.as[Int].headOption
  }
}
