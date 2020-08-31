package hackathon

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, GeneralizedLinearRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.log4j._

object a4Predict2 {
  def main (args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.config("spark.sql.warehouse.dir", "C://temp").master("local[2]").appName("a4Predict2").getOrCreate()
    import spark.implicits._
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val schem = StructType(Array(StructField("L",StringType,true)))
    val assembler = new VectorAssembler().setInputCols(Array("c1","c2","c3","c4","c5")).setOutputCol("c0")
    val glr = new GeneralizedLinearRegression().setFeaturesCol("c0").setLabelCol("c6").setMaxIter(10).setRegParam(0.01)

    val udfF = udf((s:String) => {s.substring(0,1)})
    val udfC = udf((s:String) => {s.length - " ".r.replaceAllIn(s, "").length})

    // var df0 = spark.readStream.option("sep","|").option("header",false).schema(schem).csv("D://0//Hack//2005-DevPost-Online-SparkAI//Mlib//S//").toDF() // "|"
    var df0 = spark.readStream.option("sep","|").option("header",false).schema(schem).csv("C://Programs//xampp//htdocs//sparkai//posts//").toDF()

    df0 = df0.na.fill(0)
    df0 = df0.withColumn("F", udfF(df0("L")))
    println(" A ===========================================")
    /*
    if (df0.filter($"F"==="A").count() > 0) {
        df0.withColumn("_tmp", split($"L", "\\,")).select(
            $"_tmp".getItem(1).cast(DoubleType).as("c1"),
            $"_tmp".getItem(2).cast(DoubleType).as("c2"),
            $"_tmp".getItem(3).cast(DoubleType).as("c3"),
            $"_tmp".getItem(4).cast(DoubleType).as("c4"),
            $"_tmp".getItem(5).cast(DoubleType).as("c5"),
            $"_tmp".getItem(6).cast(DoubleType).as("c9")).toDF()
            .writeStream.outputMode("append").format("console").queryName("A").start()
        }
    if (df0.filter($"F"==="B").count() > 0) {
        df0.withColumn("_tmp", split($"L", "\\,")).select(
            $"_tmp".getItem(1).cast(DoubleType).as("c1"),
            $"_tmp".getItem(2).cast(DoubleType).as("c2"),
            $"_tmp".getItem(3).cast(DoubleType).as("c3"),
            $"_tmp".getItem(4).cast(DoubleType).as("c4"),
            $"_tmp".getItem(5).cast(DoubleType).as("c5"),
            $"_tmp".getItem(6).cast(DoubleType).as("c9")).toDF()
            .writeStream.outputMode("append").format("console").queryName("B").start()
    }
    if (df0.filter($"F"==="C").count() > 0) {
        df0.withColumn("_tmp", split($"L", "\\ ")).select(
            $"_tmp".getItem(1).as("c1"),
            $"_tmp".getItem(2).as("c2"),
            $"_tmp".getItem(3).as("c3"),
            $"_tmp".getItem(4).as("c4"),
            $"_tmp".getItem(5).as("c5"),
            $"_tmp".getItem(6).cast(DoubleType).as("c9")).toDF()
            .writeStream.outputMode("append").format("console").queryName("C").start()
    }
    if (df0.filter($"F"==="D").count() > 0) {
        df0.withColumn("_tmp", split($"L", "\\ ")).select(
          $"_tmp".getItem(1).as("c1"),
          $"_tmp".getItem(2).as("c2"),
          $"_tmp".getItem(3).as("c3"),
          $"_tmp".getItem(4).as("c4"),
          $"_tmp".getItem(5).as("c5"),
          $"_tmp".getItem(6).cast(DoubleType).as("c9")).toDF()
          .writeStream.outputMode("append").format("console").queryName("D").start()
    }
    */
    if ("A" == "A") { // (df0.filter($"F"==="E").count() > 0) {
        df0.withColumn("Qty", udfC(df0("L"))).toDF()
          .writeStream.outputMode("append").format("console").queryName("E").start()
        }

    spark.streams.awaitAnyTermination()
    spark.stop()
  }}
