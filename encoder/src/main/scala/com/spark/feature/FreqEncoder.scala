package com.spark.feature

import java.lang.{String => JString, Integer => JInt}
import java.util.{NoSuchElementException, Map => JMap}

import scala.collection.JavaConverters._
import com.spark.persistence
import com.spark
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, Row}
import org.apache.spark.ml.util._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.VersionUtils.majorMinorVersion
import org.apache.spark.util.collection.OpenHashMap



/**
  * Params for [[FreqEncoder]] and [[FreqEncoderModel]].
  */
trait FreqEncoderParams extends Params {

  val inputCol= new Param[String](this, "inputCol", "The input column")

  val outputCol = new Param[String](this, "outputCol", "The output column")

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require(isDefined(inputCol), s"FreqEncoder requires input column parameter: $inputCol")
    require(isDefined(outputCol), s"FreqEncoder requires output column parameter: $outputCol")

    val field = schema.fields(schema.fieldIndex($(inputCol)))

    if (field.dataType!= StringType) {
      throw new Exception(
        s"Input type ${field.dataType} did not match input type StringType")
    }

    // Add the return field
    schema.add(StructField($(outputCol), IntegerType, false))
  }
}


class FreqEncoder(override val uid: String)
  extends Estimator[FreqEncoderModel] with FreqEncoderParams {

  def this() = this(Identifiable.randomUID("FreqEncoder"))

  def setInputCol(value: String) = set(inputCol, value)

  def setOutputCol(value: String) = set(outputCol, value)

  override def copy(extra: ParamMap): FreqEncoder = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  def countByValue(
      dataset: Dataset[_],
      inputCol: String): OpenHashMap[String, Long] = {

    inputCols = [inputCol]
    val aggregator = new StringIndexerAggregator(inputCols.length)

    implicit val encoder = Encoders.kryo[OpenHashMap[String, Long]]

    val selectedCol = inputCol.map {
      val col = dataset.col(inputCol)
      if (col.expr.dataType == StringType) {
        col
      } else {
        // We don't count for NaN values. Because `StringIndexerAggregator` only processes strings,
        // we replace NaNs with null in advance.
        new Column(If(col.isNaN.expr, Literal(null), col.expr)).cast(StringType)
      }
    }

    dataset.select(selectedCol: _*)
      .toDF
      .groupBy().agg(aggregator.toColumn)
      .as[OpenHashMap[String, Long]]
      .collect()(0)
  }

  override def fit(dataset: Dataset[_]): FreqEncoderModel = {
    transformSchema(dataset.schema, logging = true)

    val input = dataset.select($(inputCol)).rdd.map(_.getAs[String](0)).collect()

    val bins = countByValue(dataset, inputCol)

    val model = new FreqEncoderModel(uid, bins)
    copyValues(model)
  }
}


class FreqEncoderModel(override val uid: String, val bins: Map[String, Int])
  extends Model[FreqEncoderModel] with FreqEncoderParams with MLWritable {

  import FreqEncoderModel._

  /** Java-friendly version of [[bins]] */
  def javaBins: JMap[JString, JInt] = {
    bins.map{ case (k, v) => k -> v }.asJava
  }

  /** Returns the corresponding bin on which the input falls */
  /** val getBin = (a: Double, bins: SortedMap[Double, Int]) => bins.to(a).last._2 */

  val getBin = (a: String, bins: OpenHashMap[String, Long]) => bins.to(a).map(_._1).toArray

  override def copy(extra: ParamMap): FreqEncoderModel = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private var broadcastBins: Option[Broadcast[Map[String, Int]]] = None

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    if (broadcastBins.isEmpty) {
      val dict = bins
      broadcastBins = Some(dataset.sparkSession.sparkContext.broadcast(dict))
    }

    val binsBr = broadcastBins.get

    val vectorizer = udf { (input: String) => getBin(input, binsBr.value)}

    dataset.withColumn($(outputCol), vectorizer(col($(inputCol))))
  }

  override def write: MLWriter = new FreqEncoderModelWriter(this)
}


object FreqEncoderModel extends MLReadable[FreqEncoderModel] {

  class FreqEncoderModelWriter(instance: FreqEncoderModel) extends MLWriter {

    private case class Data(bins: Map[String, Int])

    override protected def saveImpl(path: String): Unit = {
      spark.persistence.DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.bins)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FreqEncoderModelReader extends MLReader[FreqEncoderModel] {

    private val className = classOf[FreqEncoderModel].getName

    override def load(path: String): FreqEncoderModel = {
      val metadata = spark.persistence.DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("bins")
        .head()

      val bins = countByValue(dataset, inputCol).map{ counts =>
          counts.toSeq.map(_._1).toArray }

      val model = new FreqEncoderModel(metadata.uid, bins)
      spark.persistence.DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[FreqEncoderModel] = new FreqEncoderModelReader

  override def load(path: String): FreqEncoderModel = super.load(path)
}


class StringIndexerAggregator(numColumns: Int)
  extends Aggregator[Row, Array[OpenHashMap[String, Long]], Array[OpenHashMap[String, Long]]] {

  override def zero: Array[OpenHashMap[String, Long]] =
    Array.fill(numColumns)(new OpenHashMap[String, Long]())

  def reduce(
      array: Array[OpenHashMap[String, Long]],
      row: Row): Array[OpenHashMap[String, Long]] = {
    for (i <- 0 until numColumns) {
      val stringValue = row.getString(i)
      // We don't count for null values.
      if (stringValue != null) {
        array(i).changeValue(stringValue, 1L, _ + 1)
      }
    }
    array
  }

  def merge(
      array1: Array[OpenHashMap[String, Long]],
      array2: Array[OpenHashMap[String, Long]]): Array[OpenHashMap[String, Long]] = {
    for (i <- 0 until numColumns) {
      array2(i).foreach { case (key: String, count: Long) =>
        array1(i).changeValue(key, count, _ + count)
      }
    }
    array1
  }

  def finish(array: Array[OpenHashMap[String, Long]]): Array[OpenHashMap[String, Long]] = array

  override def bufferEncoder: Encoder[Array[OpenHashMap[String, Long]]] = {
    Encoders.kryo[Array[OpenHashMap[String, Long]]]
  }

  override def outputEncoder: Encoder[Array[OpenHashMap[String, Long]]] = {
    Encoders.kryo[Array[OpenHashMap[String, Long]]]
  }
}
