import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._


object RegionStationMapper {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("Count").setMaster("local[2]")
    val sc = new SparkContext(sparkConfig)

    val fs: FileSystem = FileSystem.get(new Configuration())
    fs.delete(new Path(args(2)), true)

    val stations = sc.textFile(args(0)).map(line => (line.split(",")))
    val cities = sc.textFile(args(1)).map(line => line.split("\t"))

    val stationsCoordinates = stations.map(line => (round(line(2).toDouble), round(line(3).toDouble)) ->(line(0), line(1), line(2).toDouble, line(3).toDouble, line(4).toDouble))
    val regionCoordinates = cities.map(line => (round(line(4).toDouble), round(line(5).toDouble)) ->(line(8), line(4).toDouble, line(5).toDouble, line(16).toDouble))

    val joinedData = stationsCoordinates.join(regionCoordinates).map(line => line._2._1._1 ->(line._2._1._2, line._2._1._3, line._2._1._4, line._2._1._5,
      line._2._2._1, line._2._2._2, line._2._2._3, line._2._2._4,
      Math.abs(line._2._1._3 - line._2._2._2) + Math.abs(line._2._1._4 - line._2._2._3) + Math.abs(line._2._1._5 - line._2._2._4)
      ))

    val minDistancedData = joinedData.groupByKey().map(line => "%s,%s".format(line._1, {
      var stationLocation = "unknown_station"
      var regionLocation = "unknown_region"
      var min = 1000000.0
      for (arr <- line._2) {
        if (arr._9 < min) {
          min = arr._9
          stationLocation = arr._1
          regionLocation = arr._5
        }
      }
      stationLocation.concat(",").concat(regionLocation)
    }))

    minDistancedData.coalesce(1, shuffle = false).saveAsTextFile(args(2))
  }

  def round(number: Double) = {
    val res = Math.floor(number * 10.0) / 10.0
    res
  }

}