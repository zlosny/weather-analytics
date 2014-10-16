import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}


object UniqueStation {

  def collectUniqueStations(sc : SparkContext, stationsFilePath : String, outFilePath : String): Unit = {
    try {
//      val sparkConfig = new SparkConf().setAppName("UniqueStation").setMaster("local[2]")
//      val sc = new SparkContext(sparkConfig)

      val fs: FileSystem = FileSystem.get(new Configuration())
      fs.delete(new Path(outFilePath), true)

      val stations = sc.textFile(stationsFilePath).map(line => line.split(","))

      val uniqueStations = stations.map(m => "%s,%s,%s,%s,%s".format(m(0), m(1), m(3), m(4), m(2))).distinct()

      uniqueStations.coalesce(1, shuffle = false).saveAsTextFile(outFilePath)
    } catch {
      case e : Exception => throw new Exception("UniqueStationJob failed" + e.printStackTrace())
    }
  }

}
