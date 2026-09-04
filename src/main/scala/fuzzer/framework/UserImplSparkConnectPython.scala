package fuzzer.framework

import fuzzer.code.SourceCode
import fuzzer.core.exceptions.DAGFuzzerException
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.{BooleanType, DataType, DecimalType, FloatType, IntegerType, LongType, StringType}
import fuzzer.utils.random.Random
import play.api.libs.json._

import scala.sys.process._
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.io.Source

object UserImplSparkConnectPython {

  // The oracle server runs generated code with a "spark" variable already bound (it prepends
  // a classic-session or connect-session preamble depending on which mode it's executing) - so
  // unlike Dask/pandas, this codegen never constructs a session itself.
  //
  // Source reads use an absolute path rather than a relative one: the classic-mode subprocess's
  // embedded JVM shares our cwd, but Spark Connect's server does the actual file read on its own
  // side, with its own (different) cwd - a relative path silently resolves against the wrong
  // directory there. Computed once, not hardcoded, so it still works if the repo moves.
  private val projectRoot: String = new java.io.File(".").getCanonicalPath

  // ============================================================================
  // MAIN ENTRY POINTS
  // ============================================================================

  def constructDFOCall(spec: JsValue, node: Node[DFOperator], in1: String, in2: String): String = {
    val opName = node.value.name
    val opSpec = spec \ opName

    if (opSpec.isInstanceOf[JsUndefined]) {
      return opName
    }

    val opType = (opSpec \ "type").as[String]
    val parameters = (opSpec \ "parameters").as[JsObject]

    opType match {
      case "source" => generateSourceOperation(node, spec, opName, parameters)
      case "unary" => generateUnaryOperation(node, spec, opName, parameters, in1)
      case "binary" => generateBinaryOperation(node, spec, opName, parameters, in1, in2)
      case _ => s"$in1.$opName(${generateArguments(node, parameters, opType, in2).mkString(", ")})"
    }
  }

  def dag2SparkConnectPython(spec: JsValue)(graph: Graph[DFOperator]): SourceCode = {
    val preamble = generatePreamble()
    val l = mutable.ListBuffer[String]()
    val variablePrefix = "df"
    val finalVariableName = "result"

    l += "from pyspark.sql import functions as F"
    l += preamble

    graph.traverseTopological { node =>
      node.value.varName = s"$variablePrefix${node.id}"

      val call = node.getInDegree match {
        case 0 =>
          constructDFOCall(spec, node, null, null)
        case 1 => constructDFOCall(spec, node, node.parents.head.value.varName, null)
        case 2 => constructDFOCall(spec, node, node.parents.head.value.varName, node.parents.last.value.varName)
      }

      val lhs = if (node.isSink) s"$finalVariableName = " else s"${node.value.varName} = "
      l += s"$lhs$call"
    }

    // No trigger/print line needed here - unlike Dask/pandas, the oracle server grabs
    // namespace["result"] directly and calls .collect() on it itself.

    val code = l.mkString("\n")
    SourceCode(src = code, ast = null, preamble = preamble)
  }

  // ============================================================================
  // UDF GENERATORS
  // ============================================================================

  // mapInPandas needs a per-partition iterator function, but the preloaded UDF pool (shared
  // shape with Dask's mapPartitionsUdf / pandas' dfPipeUdf) is just a plain df->df transform -
  // this wraps it into the iterator form mapInPandas actually requires.
  def generatePreamble(): String = {
    s"""
       |${generatePreloadedUDF()}
       |def mapInPandasUdf(iterator):
       |    for pdf in iterator:
       |        yield dfTransformUdf(pdf)
       |""".stripMargin
  }

  def generatePreloadedUDF(): String = {
    val config = fuzzer.core.global.State.config.get

    val pythonScriptPath: String = "llm-caller/generator.py"
    val numTries: Int = 3
    val batch = (fuzzer.core.global.State.iteration / config.refreshUdfsAfter).toInt
    val outDir = s"generated/SparkConnect"
    val outPath = s"$outDir/udfs_$batch.json"
    val prompt = s"""
Generate a json file of the following format
```
{ "functions": ["def dfTransformUdf(df): ..."] }
```
The functions array should contain ${config.numUdfsPerLLMCall} Python functions. Each function should be short and simple that does something arbitrary.
Each function should be:
- Named "dfTransformUdf"
- Complete and runnable
- Contain only code (no comments or docstrings)
- Take a single pandas DataFrame argument and return a pandas DataFrame
- Should be between 1-10 lines of code
- Must preserve the DataFrame's exact columns, column order, and dtypes (this is fed into
  Spark's mapInPandas with a statically-declared output schema, so any structural change to
  columns/dtypes will cause a schema mismatch error at runtime)
""".trim

    val udfList =
      if (!Files.exists(Paths.get(outPath)) ||
        (fuzzer.core.global.State.iteration % config.refreshUdfsAfter) == 0) {

        var lastException: Throwable = null
        var attempt = 0
        var success = false

        while (attempt < numTries && !success) {
          try {
            generatePreloadedUDF(
              pythonScriptPath,
              prompt,
              outDir,
              outPath
            )
            success = true
          } catch {
            case e: Throwable =>
              lastException = e
              attempt += 1
          }
        }

        if (!success) {
          val outPathObj = Paths.get(outPath)
          if (Files.exists(outPathObj)) {
            Files.delete(outPathObj)
          }

          val previousBatchOpt =
            (0 until batch).reverse
              .map(b => s"$outDir/udfs_$b.json")
              .find(p => Files.exists(Paths.get(p)))

          previousBatchOpt match {
            case Some(prevPath) =>
              readFunctionsFromJson(prevPath)

            case None =>
              throw new DAGFuzzerException(
                s"UDF Generation failed after $numTries tries and no previous batch exists",
                lastException
              )
          }
        } else {
          readFunctionsFromJson(outPath)
        }

      } else {
        readFunctionsFromJson(outPath)
      }

    Random.choice(udfList)
  }

  def generatePreloadedUDF(pythonScriptPath: String, prompt: String, outDir: String, outPath: String): List[String] = {
    Files.createDirectories(Paths.get(outDir))

    val cmd = Seq(
      "oracle-servers/venv/bin/python",
      pythonScriptPath,
      "--prompt", prompt,
      "--out", outPath
    )

    cmd.!

    readFunctionsFromJson(outPath)
  }

  def readFunctionsFromJson(path: String): List[String] = {
    val source = Source.fromFile(path)
    try {
      val jsonStr = source.mkString
      val json = Json.parse(jsonStr)
      (json \ "functions").as[List[String]]
    } finally {
      source.close()
    }
  }

  // ============================================================================
  // OPERATION TYPE GENERATORS
  // ============================================================================

  private def generateSourceOperation(
                                       node: Node[DFOperator],
                                       spec: JsValue,
                                       opName: String,
                                       parameters: JsObject
                                     ): String = {
    val tableName = fuzzer.core.global.State.src2TableMap(node.id).identifier
    opName match {
      case "spark.read.parquet" =>
        val path = s"$projectRoot/tpcds-data-5pc/$tableName"
        s"""spark.read.parquet("$path").toDF(*[f"{c}_${node.id}" for c in spark.read.parquet("$path").columns])"""
      case _ =>
        val rows = Random.nextInt(90) + 10
        s"spark.range($rows)"
    }
  }

  private def generateUnaryOperation(
                                      node: Node[DFOperator],
                                      spec: JsValue,
                                      opName: String,
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    opName match {
      case "alias" => generateAliasOperation(node, parameters, in1)
      case "mapInPandas" => generateMapInPandasOperation(node, parameters, in1)
      case "groupBy" => generateGroupByOperation(node, parameters, in1)
      case "withColumn" => generateWithColumnOperation(node, parameters, in1)
      case "filter" => generateFilterOperation(node, parameters, in1)
      case "select" => generateSelectOperation(node, parameters, in1)
      case "orderBy" => generateOrderByOperation(node, parameters, in1)
      case "distinct" => s"$in1.distinct()"
      case "limit" => generateLimitOperation(node, parameters, in1)
      case "offset" => generateOffsetOperation(node, parameters, in1)
      case _ => generateGenericUnaryOperation(node, opName, parameters, in1)
    }
  }

  private def generateBinaryOperation(
                                       node: Node[DFOperator],
                                       spec: JsValue,
                                       opName: String,
                                       parameters: JsObject,
                                       in1: String,
                                       in2: String
                                     ): String = {
    opName match {
      case "join" => generateJoinOperation(node, parameters, in1, in2)
      case _ => generateGenericBinaryOperation(node, opName, parameters, in1, in2)
    }
  }

  // ============================================================================
  // SPECIFIC OPERATION GENERATORS
  // ============================================================================

  private def generateAliasOperation(
                                      node: Node[DFOperator],
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    val newName = Random.alphanumeric.take(8).mkString
    updateSourceState(node, parameters \ "alias", "alias", "str", newName)
    propagateState(node)
    s"$in1.alias('$newName')"
  }

  // Spark's mapInPandas needs its output schema declared upfront (unlike Dask/pandas, which
  // infer dtypes dynamically). Tried hand-constructing a DDL string from stateView's tracked
  // columns first - but stateView is a Map with no guaranteed ordering, so the declared order
  // didn't reliably match the DataFrame's real physical column order. That mismatch didn't
  // fail fast: verified empirically that classic mode tolerates it (matches by the returned
  // pandas DataFrame's own column names) while Spark Connect's mapInPandas just hangs until
  // timeout on it. Since the preloaded UDF is required to be structure-preserving, in1.schema -
  // the DataFrame's own live schema at this point - is correct by construction and can never
  // drift out of sync the way our own bookkeeping did.
  private def generateMapInPandasOperation(
                                            node: Node[DFOperator],
                                            parameters: JsObject,
                                            in1: String
                                          ): String = {
    s"$in1.mapInPandas(mapInPandasUdf, schema=$in1.schema)"
  }

  private def constructAggFollowup(node: Node[DFOperator]): String = {
    val (_, col) = pickRandomColumnFromReachableSources(node)
    val aggFuncs = Seq("sum", "avg", "count", "min", "max")
    val fn = aggFuncs(Random.nextInt(aggFuncs.length))
    val resultColName = s"$fn(${col.name})"

    filterColumns(col.name, node)
    renameAggregatedColumn(col.name, resultColName, node)
    propagateState(node)

    s"agg({'${col.name}': '$fn'})"
  }

  private def generateGroupByOperation(
                                        node: Node[DFOperator],
                                        parameters: JsObject,
                                        in1: String
                                      ): String = {
    val groupCol = pickRandomColumnFromReachableSources(node)._2.name
    s"$in1.groupBy('$groupCol').${constructAggFollowup(node)}"
  }

  private def generateColumnExpr(col: ColumnMetadata, dfVar: String): String = {
    col.dataType match {
      case IntegerType | LongType =>
        Random.choice(List(s"$dfVar['${col.name}'] * 2", s"$dfVar['${col.name}'] + ${Random.nextInt(100)}", s"-$dfVar['${col.name}']"))
      case FloatType | DecimalType =>
        Random.choice(List(s"$dfVar['${col.name}'] * 1.5", s"$dfVar['${col.name}']"))
      case StringType =>
        s"F.upper($dfVar['${col.name}'])"
      case BooleanType =>
        s"~$dfVar['${col.name}']"
      case _ =>
        s"$dfVar['${col.name}']"
    }
  }

  private def generateWithColumnOperation(
                                           node: Node[DFOperator],
                                           parameters: JsObject,
                                           in1: String
                                         ): String = {
    val newColName = Random.alphanumeric.take(8).mkString
    val (_, col) = pickRandomColumnFromReachableSources(node)
    val expr = generateColumnExpr(col, in1)

    updateSourceState(node, parameters \ "colName", "colName", "str", newColName)
    propagateState(node)

    s"$in1.withColumn('$newColName', $expr)"
  }

  def generateFilterPredicate(node: Node[DFOperator], dfVar: String): String = {
    val (_, col) = pickRandomColumnFromReachableSources(node)
    col.dataType match {
      case IntegerType | LongType =>
        val value = Random.nextInt(100)
        val ops = List(">", "<", ">=", "<=", "==", "!=")
        val op = ops(Random.nextInt(ops.length))
        s"$dfVar['${col.name}'] $op $value"
      case FloatType | DecimalType =>
        s"$dfVar['${col.name}'] > ${Random.nextFloat() * 100}"
      case StringType =>
        s"F.length($dfVar['${col.name}']) > 5"
      case BooleanType =>
        if (Random.nextBoolean()) s"$dfVar['${col.name}']" else s"~$dfVar['${col.name}']"
      case _ =>
        s"$dfVar['${col.name}'].isNotNull()"
    }
  }

  private def generateFilterOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    s"$in1.filter(${generateFilterPredicate(node, in1)})"
  }

  private def generateSelectOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val cols = getAllColumns(node).map(_._2.name).take(Random.nextInt(3) + 1)
    filterColumns(cols.mkString(","), node)
    propagateState(node)
    s"$in1.select(${cols.map(c => s"'$c'").mkString(", ")})"
  }

  private def generateOrderByOperation(
                                        node: Node[DFOperator],
                                        parameters: JsObject,
                                        in1: String
                                      ): String = {
    val cols = getAllColumns(node).map(_._2.name).take(Random.nextInt(2) + 1)
    val ascending = Random.choice(List("True", "False"))
    s"$in1.orderBy(${cols.map(c => s"'$c'").mkString(", ")}, ascending=$ascending)"
  }

  private def generateLimitOperation(
                                      node: Node[DFOperator],
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    val n = Random.nextInt(100) + 1
    s"$in1.limit($n)"
  }

  private def generateOffsetOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val n = Random.nextInt(20)
    s"$in1.offset($n)"
  }

  private def generateJoinOperation(
                                     node: Node[DFOperator],
                                     parameters: JsObject,
                                     in1: String,
                                     in2: String
                                   ): String = {
    val joinTypes = List("inner", "outer", "left", "right")
    val how = joinTypes(Random.nextInt(joinTypes.length))

    val (leftCol, rightCol) = pickJoinColumnPair(node)
    s"$in1.join($in2, on=$in1['${leftCol.name}'] == $in2['${rightCol.name}'], how='$how')"
  }

  // Same fix as UserImplDaskPython/UserImplPandasPython's pickJoinColumnPair: constrain each
  // side's candidate columns to what's actually reachable via that specific parent, rather than
  // sampling from the join node's unified (both-sides-merged) stateView.
  private def pickJoinColumnPair(node: Node[DFOperator]): (ColumnMetadata, ColumnMetadata) = {
    val leftSourceIds = node.parents.head.getReachableSources.map(_.id).toSet
    val rightSourceIds = node.parents.last.getReachableSources.map(_.id).toSet

    val leftCols = node.value.stateView.collect { case (id, t) if leftSourceIds.contains(id) => t.columns }.flatten.toList
    val rightCols = node.value.stateView.collect { case (id, t) if rightSourceIds.contains(id) => t.columns }.flatten.toList

    assert(leftCols.nonEmpty && rightCols.nonEmpty,
      s"Expected both join sides to have reachable columns: stateView = ${node.value.stateView}")

    val leftByType = leftCols.groupBy(_.dataType)
    val rightByType = rightCols.groupBy(_.dataType)
    val commonTypes = leftByType.keySet.intersect(rightByType.keySet).toList

    if (commonTypes.nonEmpty) {
      val dt = Random.choice(commonTypes)
      (Random.choice(leftByType(dt)), Random.choice(rightByType(dt)))
    } else {
      (Random.choice(leftCols), Random.choice(rightCols))
    }
  }

  private def generateGenericUnaryOperation(
                                             node: Node[DFOperator],
                                             opName: String,
                                             parameters: JsObject,
                                             in1: String
                                           ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.$opName(${args.mkString(", ")})"
  }

  private def generateGenericBinaryOperation(
                                              node: Node[DFOperator],
                                              opName: String,
                                              parameters: JsObject,
                                              in1: String,
                                              in2: String
                                            ): String = {
    val args = generateArguments(node, parameters, "binary", in2)
    s"$in1.$opName(${args.mkString(", ")})"
  }

  // ============================================================================
  // PARAMETER AND ARGUMENT GENERATION (generic fallback - not exercised by this spec
  // today, since every current operator is hand-cased above, but kept for parity/future ops)
  // ============================================================================

  def generateArguments(
                         node: Node[DFOperator],
                         parameters: JsObject,
                         opType: String,
                         in2: String
                       ): List[String] = {
    val paramNames = parameters.keys.toList

    paramNames.flatMap { paramName =>
      val param = parameters \ paramName
      val paramType = getParamType((param \ "type").toOption.getOrElse(JsString("str")))
      val required = (param \ "required").as[Boolean]
      val hasDefault = (param \ "default").isDefined

      if (paramName == "other" && paramType == "DataFrame" && opType == "binary") {
        Some(in2)
      } else if (required || (!hasDefault && Random.nextBoolean())) {
        Some(s"$paramName=${generateRandomValue(node, param, paramType, paramName)}")
      } else {
        None
      }
    }
  }

  def generateRandomValue(
                           node: Node[DFOperator],
                           param: JsLookupResult,
                           paramType: String,
                           paramName: String
                         ): String = {
    val allowedValues: Option[Seq[JsValue]] = (param \ "values").asOpt[Seq[JsValue]]

    allowedValues match {
      case Some(values) if values.nonEmpty =>
        val randomValue = values(Random.nextInt(values.length))
        randomValue match {
          case JsString(str) => s"'$str'"
          case other => other.toString
        }

      case _ =>
        paramType match {
          case "int" => Random.nextInt(100).toString
          case "bool" => if (Random.nextBoolean()) "True" else "False"
          case "str" | "string" => s"'${Random.alphanumeric.take(8).mkString}'"
          case "list[str]" =>
            val cols = getAllColumns(node).map(_._2.name).take(Random.nextInt(2) + 1)
            s"[${cols.map(c => s"'$c'").mkString(", ")}]"
          case "Column" => generateFilterPredicate(node, "df")
          case _ => s"'${Random.alphanumeric.take(8).mkString}'"
        }
    }
  }

  def getParamType(typeJson: JsValue): String = {
    typeJson match {
      case JsString(t) => t
      case JsArray(types) => types(Random.nextInt(types.length)).as[String]
      case _ => "str"
    }
  }

  // ============================================================================
  // COLUMN SELECTION UTILITIES
  // ============================================================================

  def getAllColumns(node: Node[DFOperator]): Seq[(TableMetadata, ColumnMetadata)] = {
    node.value.stateView.values.toSeq.flatMap { t =>
      t.columns.map(c => (t, c))
    }
  }

  def pickRandomColumnFromReachableSources(node: Node[DFOperator]): (TableMetadata, ColumnMetadata) = {
    val tablesColPairs = getAllColumns(node).filter {
      case (_, col) =>
        col.metadata.get("gen-iteration") match {
          case None => true
          case Some(i) => fuzzer.core.global.State.iteration.toString != i
        }
    }

    assert(tablesColPairs.nonEmpty, s"Expected columnNames to be non-empty: stateViewMap = ${node.value.stateView}")
    tablesColPairs(Random.nextInt(tablesColPairs.length))
  }

  // ============================================================================
  // STATE MANAGEMENT
  // ============================================================================

  def renameTables(newValue: String, node: Node[DFOperator]): Unit = {
    val dfOp = node.value
    val renamedStateView: Map[String, TableMetadata] = dfOp.stateView.map {
      case (id, tableMeta) =>
        val renamed = tableMeta.copy()
        renamed.setIdentifier(newValue)
        id -> renamed
    }
    dfOp.stateView = renamedStateView
  }

  def addColumn(value: String, node: Node[DFOperator]): Unit = {
    node.value.stateView = node.value.stateView + ("added" -> TableMetadata(
      _identifier = "",
      _columns = Seq(ColumnMetadata(name = value, dataType = DataType.generateRandom, metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.core.global.State.iteration.toString))),
      _metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.core.global.State.iteration.toString)
    ))
  }

  def filterColumns(columns: String, node: Node[DFOperator]): Unit = {
    val colNames = columns.split(",").map(_.trim)
    node.value.stateView = node.value.stateView.map {
      case (tname, tmd) =>
        (tname -> tmd.filterColumns(colNames))
    }
  }

  // Not used by table-level rename ("alias" here uses table-rename) - kept for parity with
  // the other frameworks' updateSourceState shape. None of this spec's operators trigger it.
  private def renameColumn(oldName: String, newName: String, node: Node[DFOperator]): Unit = {
    node.value.stateView = node.value.stateView.map { case (id, tmd) =>
      id -> TableMetadata(tmd.identifier, tmd.columns.map(c => if (c.name == oldName) c.copy(name = newName) else c), tmd.metadata)
    }
  }

  // pyspark's .agg({'col': fn}) renames the aggregated column to "fn(col)" - state needs to
  // track that or every downstream reference to the pre-aggregation name would be wrong.
  private def renameAggregatedColumn(oldName: String, newName: String, node: Node[DFOperator]): Unit =
    renameColumn(oldName, newName, node)

  def updateSourceState(
                         node: Node[DFOperator],
                         param: JsLookupResult,
                         paramName: String,
                         paramType: String,
                         paramVal: String
                       ): Unit = {
    val effect = (param \ "state-effect").asOpt[String].getOrElse("")
    effect match {
      case "table-rename" => renameTables(paramVal, node)
      case "column-add" => addColumn(paramVal, node)
      case "column-filter" => filterColumns(paramVal, node)
      case "column-rename" => // Not used by the current spec (alias uses table-rename); no-op like the other frameworks
      case _ => // No state change
    }
  }

  def propagateState(startNode: Node[DFOperator]): Unit = {
    val visited = mutable.Set[String]()
    val queue = mutable.Queue[Node[DFOperator]]()
    queue.enqueueAll(startNode.children)

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (!visited.contains(current.id)) {
        visited += current.id

        val currentDFOp = current.value
        val startStateView = startNode.value.stateView

        val updatedView = currentDFOp.stateView.map {
          case (key, _) if startStateView.contains(key) =>
            key -> startStateView(key).copy()
          case other =>
            other
        }

        currentDFOp.stateView = updatedView
        queue.enqueueAll(current.children)
      }
    }
  }
}
