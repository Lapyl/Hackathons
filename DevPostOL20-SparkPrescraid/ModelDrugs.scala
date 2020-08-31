package hackathon

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{GBTRegressor, RandomForestRegressor}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

object a8ModelsDrugs extends Serializable {
  def main (args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.config("spark.sql.warehouse.dir", "C://temp").master("local[1]").appName("a8ModelsDrugs").getOrCreate()
    import spark.implicits._

    val udfDiv = udf((iI:Double, jJ:Double) => { if (jJ==0.00) { iI } else { (Math.round( (100*iI)/jJ ))/100.00 } })
    val udfSqd = udf((iI:Double, jJ:Double) => { (iI - jJ)*(iI - jJ) })

    var df0 = spark.read.format("com.databricks.spark.csv").option("sep","\t").option("header","true").option("mode","DROPMALFORMED").load("D://0//Hack//2005-DevPost-Online-SparkAI//Drugs//drugsComTrain_raw.tsv")
    df0 = df0.na.drop()
    // df0.show()

    val stringIndexer1 = new StringIndexer().setInputCol("drugName").setOutputCol("drugNameX").fit(df0)
    var df1 = stringIndexer1.transform(df0).drop("drugName")
    // df1.show()

    val stringIndexer2 = new StringIndexer().setInputCol("condition").setOutputCol("conditionX").fit(df0)
    df1 = stringIndexer2.transform(df1).drop("condition")
    // df1.show()

    df1 = df1.withColumn("ratingX", df1("rating").cast(DoubleType))
    // df1.show()

    df1 = df1.withColumn("usefulCountX", df1("usefulCount").cast(DoubleType))
    // df1.show()

    df1 = df1.withColumn("raingIndex", udfDiv(df1("ratingX"), df1("usefulCountX")))
    // df1.show()

    df1 = df1.select("drugNameX", "conditionX", "raingIndex")
    // df1.show()

    var vectorAssembler = new VectorAssembler().setInputCols(Array("drugNameX", "conditionX")).setOutputCol("features")
    df1 = vectorAssembler.transform(df1).select("features", "raingIndex")
    df1.show()

    var dfS = df1.randomSplit(Array(0.75, 0.25), seed = 11L)

    var nam = List("GBT", "RF")
    var par = List(Map("eta" -> 0.1f, "objective" -> "multi:softprob", "num_class" -> 3, "num_round" -> 100, "num_workers" -> 2, "max_depth" -> 2, "num_early_stopping_rounds" -> 10),
                   Map("eta" -> 0.1f, "objective" -> "multi:softprob", "num_class" -> 3, "num_round" -> 100, "num_workers" -> 2, "max_depth" -> 2, "num_early_stopping_rounds" -> 10))

    for (j <- 0 to 1) {

      var reg = List ( new GBTRegressor ().setFeaturesCol ( "features" ).setLabelCol ( "raingIndex" ), new RandomForestRegressor ().setFeaturesCol ( "features" ).setLabelCol ( "raingIndex" ) )( j )

      var mod = reg.fit ( dfS ( 0 ) )
      mod.write.overwrite ().save ( "D://0//Hack//2005-DevPost-Online-SparkAI//Drugs//Models//" + nam ( j ) + "//" )

      var res = mod.transform ( dfS ( 1 ) )
      res = res.withColumn ( "uni", lit ( 1 ) )
        .withColumn ( "dev", udfSqd ( res ( "raingIndex" ), res ( "prediction" ) ) )

      res = res.groupBy ().sum ( "dev", "uni" )
      res.show ()
    }
    spark.stop()
  }}
