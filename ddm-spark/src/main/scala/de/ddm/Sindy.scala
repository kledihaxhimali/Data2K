package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import java.io.{BufferedWriter, File, FileWriter}

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val avDatasets: Seq[Dataset[Row]] = inputs.filter(_.nonEmpty).map { path =>
      val df = readData(path, spark)

      val tableName = new File(path).getName.stripSuffix(".csv")

      val cols = df.columns
      if (cols.isEmpty) {
        spark.emptyDataFrame.toDF("attribute", "value")
      } else {
        val stackExpr =
          s"stack(${cols.length}, " +
            cols.map(c => s"'${tableName}.${c}', `${c}`").mkString(", ") +
            ") as (attribute, value)"

        df.selectExpr(stackExpr)
          .select(
            $"attribute",
            trim($"value".cast(StringType)).as("value")
          )
          .filter($"value".isNotNull && length($"value") > 0)
          .distinct()
      }
    }

    val attributeValues =
      if (avDatasets.isEmpty) spark.emptyDataFrame.toDF("attribute", "value")
      else avDatasets.reduce(_ union _).repartition($"value")

    val valueToAttrs =
      attributeValues
        .groupBy($"value")
        .agg(collect_set($"attribute").as("attrs"))

    val dependentCandidates =
      valueToAttrs
        .select(explode($"attrs").as("dependent"), $"attrs".as("candidates"))

    val perDependent =
      dependentCandidates
        .groupBy($"dependent")
        .agg(collect_list($"candidates").as("cands"))

    val dependentToReferenced =
      perDependent
        .select(
          $"dependent",
          expr(
            """
              |aggregate(
              |  cands,
              |  cands[0],
              |  (acc, x) -> array_intersect(acc, x)
              |)
              |""".stripMargin
          ).as("refsAll")
        )
        .select(
          $"dependent",
          expr("filter(refsAll, x -> x <> dependent)").as("refs")
        )

    val inds: Dataset[(String, String)] =
      dependentToReferenced
        .select($"dependent", explode($"refs").as("referenced"))
        .as[(String, String)]
        .distinct()

    createOutput(inds)
  }

  private def createOutput(dataset: Dataset[(String, String)]): Unit = {
    // Collect, stringify and sort INDs
    val inds = dataset.collect().map(ind => ind._1 + " c " + ind._2).sorted

    // Print results to the console
    inds.foreach(println(_))

    // Write results into a result file
    val writer = new BufferedWriter(new FileWriter(new File("result.txt")))
    inds.foreach(s => writer.write(s + "\r\n"))
    writer.close()
  }
}
