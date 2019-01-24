import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.SizeEstimator

object  Semantix {
  def main(args: Array[String]){

    val conf = new SparkConf().set("spark.executor.memory", "2g").setMaster("local").setAppName("Semantix")
    val sc = new SparkContext(conf)

    var julho = sc.textFile("/Users/guilhermemoreira/Documents/Projects/nasa/access_log_Jul95")
    julho.cache()

    var agosto = sc.textFile("/Users/guilhermemoreira/Documents/Projects/nasa/access_log_Aug95")
    agosto.cache()


    var julho_404_erro = julho.filter(line => line.contains("404")).cache()
    var agosto_404_erro = agosto.filter(line => line.contains("404")).cache()
    hostsunicos(julho,"Julho")
    hostsunicos(agosto,"Agosto")
    total404(julho_404_erro,"julho")
    total404(agosto_404_erro,"agosto")
    urls5top(julho_404_erro,"Julho")
    urls5top(agosto_404_erro,"agosto")
    pordia404(julho_404_erro,"Julho")
    pordia404(agosto_404_erro,"agosto")
    println(SizeEstimator.estimate(julho))
    println(SizeEstimator.estimate(agosto))
    sc.stop()
  }

  // Contagem de hosts
  def hostsunicos(rdd: RDD[String],mes:String) = {
    var dt = rdd.flatMap(line => line).distinct().count()
    println("Mostrado hosts distintos ", mes + " " +  dt)

  }

  // Contagem de 404 error
  def total404(rdd: RDD[String],mes:String) = {
    println("Erros 404 em  " + mes + " " + rdd.count())
  }

  // Contagem de 5 404
  def urls5top(rdd: RDD[String],mes:String) = {
    var urls = rdd.map( line => line.split('"')(1).split(' ')(1))
    var counts = urls.map( endpoint => (endpoint, 1)).reduceByKey((a, b) => a + b)
    var top = counts.sortBy( pair => pair._2).take(5)
    top.foreach(x => print(x + "\n"))
  }

  // 404 diario
  def pordia404(rdd: RDD[String],mes:String) = {
    var days = rdd.map( line => line.split('[')(1).split(':')(0))
    var counts = days.map( day => (day, 1)).reduceByKey((a, b) => a + b).collect()

    print("\n404 errors diario:\n")
    counts.foreach(x => print(x + "\n"))
  }
}