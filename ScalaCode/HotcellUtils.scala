package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  // Configuration variables
  val coordinateStep = 0.01
  val sumNeighbors = 26

  // Calculate coordinate based on input string and coordinate offset
  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble / coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble / coordinateStep).toInt
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  // Parse timestamp string to Timestamp object
  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  // Get the day of year from the timestamp
  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  // Get the day of month from the timestamp
  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // Calculate the G-score for a given cell
  def calculateGsordScore(sumPickupPoint: Int, sumNeighbors: Int, numCells: Int, mean: Double, stdDev: Double): Double =
  {
    var numerator = sumPickupPoint.toDouble - (mean * sumNeighbors.toDouble)
    var denominator = stdDev * math.sqrt(((numCells.toDouble * sumNeighbors.toDouble) - math.pow(sumNeighbors.toDouble, 2)) / (numCells.toDouble - 1.0))
    var output = (numerator / denominator).toDouble

    return output
  }

  // Calculate the number of neighbors for a given cell
  def calculateNeighbors(valX: Int, valY: Int, valZ: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int): Int = {
    val neighborCheck = Seq(
      if (valX == minX || valX == maxX) 1 else 0,
      if (valY == minY || valY == maxY) 1 else 0,
      if (valZ == minZ || valZ == maxZ) 1 else 0
    ).sum

    var result = 0

    if (neighborCheck == 0) {
      result = 0
    } else if (neighborCheck == 1) {
      result = 9
    } else if (neighborCheck == 2) {
      result = 15
    } else if (neighborCheck == 3) {
      result = 19
    }

    sumNeighbors - result
  }
}
