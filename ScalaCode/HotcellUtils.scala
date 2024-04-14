package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01
  val sumNeighbors = 26

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def calculateGsordScore(sumPickupPoint: Int, sumNeighbors: Int, numCells: Int, mean: Double, stdDev: Double): Double =
  {
    var numerator = sumPickupPoint.toDouble - (mean * sumNeighbors.toDouble)
    var denominator = stdDev * math.sqrt(((numCells.toDouble * sumNeighbors.toDouble) - math.pow(sumNeighbors.toDouble, 2)) / (numCells.toDouble - 1.0))
    var output = (numerator/denominator).toDouble

  return output
  }

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
