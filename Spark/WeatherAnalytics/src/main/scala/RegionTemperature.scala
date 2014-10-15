import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._


object RegionTemperature {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("Count").setMaster("local[2]")
    val sc = new SparkContext(sparkConfig)

    val fs: FileSystem = FileSystem.get(new Configuration())
    fs.delete(new Path(args(2)), true)
    fs.delete(new Path(args(3)), true)

    val weatherFile = sc.textFile(args(0)).map(line => (line.split(",")))
    val regionsFile = sc.textFile(args(1)).map(line => line.split(","))

    val weatherData = weatherFile.map(line => line(0) ->(line(5), line(9).toDouble, line(10).toDouble))
    val regionsData = regionsFile.map(line => line(0) ->(line(1), line(2)))

    val allWeather = weatherData.join(regionsData).map(line => line._2._2._2 ->
      (line._2._1._1, line._2._1._2, line._2._1._3, meanTemperature(line._2._1._2, line._2._1._3)))

    val countryMaxT = allWeather.map(line => line._1 -> line._2._2).filter(line => line._2 != -9999).groupByKey().map(line => line._1 -> line._2.max)
    val countryMinT = allWeather.map(line => line._1 -> line._2._3).filter(line => line._2 != -9999).groupByKey().map(line => line._1 -> line._2.min)
    val countryMeanT = allWeather.map(line => line._1 -> line._2._4).filter(line => line._2 != -9999).groupByKey().map(line => line._1 -> (line._2.sum / line._2.count(line => 1 == 1)))

    val conditionWeatherRegion = countryMaxT.join(countryMinT).join(countryMeanT).map(line => "%s,%s,%s,%s".format(line._1, line._2._1._1, line._2._1._2, line._2._2))
    val allWeatherRegions = allWeather.map(line => "%s,%s,%s,%s,%s".format(line._1, line._2._1, line._2._2, line._2._3, line._2._4))

    allWeatherRegions.coalesce(1, shuffle = false).saveAsTextFile(args(2))
    conditionWeatherRegion.coalesce(1, shuffle = false).saveAsTextFile(args(3))
  }

  def meanTemperature(minT: Double, maxT: Double): Double = {
    var result: Double = 0.0
    if (minT == -9999 || maxT == -9999) {
      result = -9999
    } else {
      result = (minT + maxT) / 2.0
    }
    result
  }

}
