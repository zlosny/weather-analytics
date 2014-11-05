import org.apache.spark.{SparkContext, SparkConf}

object WeatherAnalyticsJobRunner {

  def main(args: Array[String]) {
    try {
      val sparkConfig = new SparkConf().setAppName("WeatherAnalyticsJob").setMaster("local[2]")
      val sc = new SparkContext(sparkConfig)

      UniqueStation.collectUniqueStations(sc, args(0), args(2))
      RegionStationMapper.regionStationMerge(sc, args(2), args(1), args(3))
      RegionTemperature.regionTemperatureAnalytics(sc, args(0), args(3), args(4), args(5))
    } catch {
      case e: Exception => print(e.getLocalizedMessage)
    }
  }

}
