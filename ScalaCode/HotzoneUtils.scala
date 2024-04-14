package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    try {
      val rectCoords = queryRectangle.split(",")
      val pointCoords = pointString.split(",")

      val x1 = rectCoords(0).toDouble
      val y1 = rectCoords(1).toDouble
      val x2 = rectCoords(2).toDouble
      val y2 = rectCoords(3).toDouble

      val px = pointCoords(0).toDouble
      val py = pointCoords(1).toDouble

      px >= Math.min(x1, x2) && px <= Math.max(x1, x2) && py >= Math.min(y1, y2) && py <= Math.max(y1, y2)
    } catch {
      case _: Throwable => false
    }
  }

}
