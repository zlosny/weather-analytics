import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._


object RegionTemperature {

  def regionTemperatureAnalytics(sc : SparkContext, weatherFilePath : String, regionStationFilePath : String, allAnalyticsFilePath  : String, dayAnalyticsFilePath : String): Unit = {
    try {
//      val sparkConfig = new SparkConf().setAppName("RegionTemperature").setMaster("local[2]")
//      val sc = new SparkContext(sparkConfig)

      val fs: FileSystem = FileSystem.get(new Configuration())
      fs.delete(new Path(allAnalyticsFilePath), true)
      fs.delete(new Path(dayAnalyticsFilePath), true)

      val weatherFile = sc.textFile(weatherFilePath).map(line => (line.split(",")))
      val regionsFile = sc.textFile(regionStationFilePath + "part-*").map(line => line.split(","))

      val weatherData = weatherFile.map(line => line(0) ->(line(5), line(9).toDouble, line(10).toDouble))
      val regionsData = regionsFile.map(line => line(0) ->(line(1), line(2)))

      val allWeather = weatherData.join(regionsData).map(line => line._2._2._2 ->
        (line._2._1._1, line._2._1._2, line._2._1._3, meanTemperature(line._2._1._2, line._2._1._3)))

      val countryMaxT = allWeather.map(line => line._1 -> line._2._2).filter(line => line._2 != -9999).groupByKey().map(line => line._1 -> line._2.max)
      val countryMinT = allWeather.map(line => line._1 -> line._2._3).filter(line => line._2 != -9999).groupByKey().map(line => line._1 -> line._2.min)
      val countryMeanT = allWeather.map(line => line._1 -> line._2._4).filter(line => line._2 != -9999).groupByKey().map(line => line._1 -> (line._2.sum / line._2.count(line => 1 == 1)))

      val dayWeatherRegion = countryMaxT.join(countryMinT).join(countryMeanT).map(line => "%s,%s,%s,%s".format(line._1, line._2._1._1, line._2._1._2, line._2._2))
      val allWeatherRegions = allWeather.map(line => "%s,%s,%s,%s,%s".format(line._1, line._2._1, line._2._2, line._2._3, line._2._4))

      allWeatherRegions.coalesce(1, shuffle = false).saveAsTextFile(allAnalyticsFilePath)
      dayWeatherRegion.coalesce(1, shuffle = false).saveAsTextFile(dayAnalyticsFilePath)
    } catch {
      case e : Exception => throw new Exception("RegionTemperatureAnalyticsJob failed" + e.printStackTrace())
    }
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
