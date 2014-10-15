import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}


object UniqueStation {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setAppName("Count").setMaster("local[2]")
    val sc = new SparkContext(sparkConfig)

    val fs: FileSystem = FileSystem.get(new Configuration())
    fs.delete(new Path(args(1)), true)

    val stations = sc.textFile(args(0)).map(line => line.split(","))

    val uniqueStations = stations.map(m => "%s,%s,%s,%s,%s".format(m(0), m(1), m(3), m(4), m(2))).distinct()

    uniqueStations.coalesce(1, shuffle = false).saveAsTextFile(args(1))
  }

}
