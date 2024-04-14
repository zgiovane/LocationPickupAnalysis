package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  // Set logging levels to WARN
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  // Method to perform Hotspot analysis
  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    pickupInfo.createOrReplaceTempView("pickupInfo")

    spark.udf.register("calculateNeighbors", HotcellUtils.calculateNeighbors _)

    val hotCellPoints = spark.sql(
      s"""
        SELECT x, y, z, COUNT(*) AS pickupCount
        FROM pickupInfo
        WHERE x >= $minX AND x <= $maxX AND y >= $minY AND y <= $maxY AND z >= $minZ AND z <= $maxZ
        GROUP BY z, y, x
      """.stripMargin)
    hotCellPoints.createOrReplaceTempView("hotCellPoints")

    val XjSquared = spark.sql(
      s"""
        SELECT SUM(pickupCount) AS sumOfXj, SUM(pickupCount * pickupCount) AS sqsumOfXj
        FROM hotCellPoints
      """.stripMargin)
    XjSquared.createOrReplaceTempView("XjSquared")

    val XjSquaredRow = XjSquared.first()
    val sumXj = XjSquaredRow.getLong(0).toDouble
    val XjSquaredSum = XjSquaredRow.getLong(1).toDouble

    val xMean = sumXj / numCells.toDouble
    val stdDev = math.sqrt((XjSquaredSum / numCells.toDouble) - (xMean * xMean))

    val neighborCells = spark.sql(
      s"""
        SELECT
          calculateNeighbors(p1.x, p1.y, p1.z, $minX, $minY, $minZ, $maxX, $maxY, $maxZ) AS neighborCellCount,
          p1.x AS x, p1.y AS y, p1.z AS z,
          SUM(p2.pickupCount) AS attrSpaWeight
        FROM hotCellPoints AS p1, hotCellPoints AS p2
        WHERE (p2.x = p1.x - 1 OR p2.x = p1.x OR p2.x = p1.x + 1)
          AND (p2.y = p1.y - 1 OR p2.y = p1.y OR p2.y = p1.y + 1)
          AND (p2.z = p1.z - 1 OR p2.z = p1.z OR p2.z = p1.z + 1)
        GROUP BY p1.z, p1.y, p1.x
      """.stripMargin)
    neighborCells.createOrReplaceTempView("neighborCells")

    spark.udf.register("GetisOrdStat",
      (attrSpaWeight: Int, neighborCellCount: Int, numCells: Int, xMean: Double, stdDev: Double) =>
        HotcellUtils.calculateGsordScore(attrSpaWeight, neighborCellCount, numCells, xMean, stdDev))

    val GetisOrdStatCells = spark.sql(
      s"""
        SELECT
          GetisOrdStat(attrSpaWeight, neighborCellCount, $numCells, $xMean, $stdDev) AS GetisOrdStat,
          x, y, z
        FROM neighborCells
        ORDER BY GetisOrdStat DESC
      """.stripMargin)
    GetisOrdStatCells.createOrReplaceTempView("GetisOrdStatCells")

    // x, y, z values sorted based on GetisOrdStat score
    val resultInfo = spark.sql("SELECT x, y, z FROM GetisOrdStatCells")
    resultInfo.createOrReplaceTempView("finalPickupInfo")

    resultInfo
  }
}
