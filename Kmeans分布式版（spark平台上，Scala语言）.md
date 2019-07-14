```
//代码是调通的，处理逻辑可以参考。
//用时需要自己导入相应的库，连接spark。
//代码依然有改进的部分，欢迎指正。

import ...
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

object KMeans {

  //计算距离函数
  def distance(x: ArrayBuffer[Double], y: ArrayBuffer[Double]) = {
    var tmpSum = 0.0
    var distresult = 0.0
    for (i <- (2 until x.length)) {
      var tmp = x(i) - y(i)
      var tmpsqrt = tmp * tmp
      tmpSum += tmpsqrt
    }
    distresult = math.sqrt(tmpSum)
    distresult
  }


//两个数组按列求和函数，用来在簇内先求和，后面求簇心坐标用。
  def sumArrayBuffer(x: ArrayBuffer[Double], y: ArrayBuffer[Double]) = {
    val tuple1: ArrayBuffer[(Double, Double)] = x.zip(y)
    val sumAB: ArrayBuffer[Double] = tuple1.map(x => x._1+x._2)
    sumAB
  }

  //计算两个簇心距离
  //在默认两个簇心都是按0 - 4的顺序排列的前提下，一一对应进行计算
  def centroidDistance (x:Array[(Int, ArrayBuffer[Double])] , y:Array[(Int, ArrayBuffer[Double])]) = {
    var result :Double = 0.0
    for(i <- 0 until x.length) {
      val tuples: ArrayBuffer[(Double, Double)] = x(i)._2.zip(y(i)._2)
      result = tuples.map( x => ((x._1-x._2)*(x._1-x._2))).sum
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val k:Int = 2
    val standardMinDistance:Double  = 0.1    //确定簇心最小偏差
    var count : Int = 0                       //统计迭代次数
    var ClusterOut:Array[(Int, ArrayBuffer[Double])] = new Array[(Int, ArrayBuffer[Double])](k)    //存放结果簇心
    var distanceCentrols : Double = 0.1   //迭代时的新旧簇心距，用于确定是否继续迭代
    var countNums = 0              //统计总数量，用于计算簇内平均距离
    var distanceInnerSum = 0.0     //统计总的类内距
    var distanceInnerAvg = 0.0     //计算平均类内距
    var distancebetweenAvg = 0.0   //计算平均类间距
    var numSumAll = 0              //总的数量，用于计算平均簇内间距
    var effort = 0.0               //簇内距/簇间距。衡量聚类效果
    val Inf:Double = 1.0 / 0.0            //无穷大

    val filePath = "F:\\testData\\data.txt"
//    val srcTable = "db1" 
//集群上用，注意修改pom文件
    //基本配置，创建RDD
//      //本地时用，注意修改pom文件
    Properties.init(Array("-model=local"))                               //本地时用
    System.setProperty("hadoop.home.dir", "D:\\tools\\winutils")         //本地时用
    val sparkContext = SparkConfiguration.getSparkContext()
    sparkContext.setLogLevel("WARN")
    val rdd_file: RDD[String] = sparkContext.textFile(filePath)          //本地时用


//    val client = new TDWClient()                                         //集群上用
//    val lines: RDD[Array[String]] = client.tdw("ieg_face").table(srcTable, priParts = Seq(
//      "p_20181016"
//    ))   //取的分区

    val lines: RDD[Array[String]] = rdd_file.map(_.split("\t"))          //本地时用

    //-----------------------数据去掉表头和前两列--------------------------
    //去掉第一行
    val linesNew: RDD[Array[String]] = lines.filter(row => !row.contains("threem_depo"))
    // 叠加从第三列往后的列，追加形式。
    val abLines: RDD[ArrayBuffer[Double]] = linesNew.map(x => {
      val ab = new ArrayBuffer[Double]() //用来存放第三列往后的数据
      for (i <- 2 until x.length) {
        ab.append(x(i).toDouble) //String转化为Double类型
      }
      ab
    })
    //---------------------------数据去掉第一行和前两列完毕--------------------------
    //---------------------------归一化----------------------------------------------
//    var maxNorm:ArrayBuffer[Double]  = new ArrayBuffer[Double]()
//    var minNorm:ArrayBuffer[Double] = new ArrayBuffer[Double]()
//    var brandNorm:ArrayBuffer[Double] = new ArrayBuffer[Double]()
//    //---------------------------1.寻找本列最大最小值---------------------------------
////    val norm: Array[ArrayBuffer[Double]] =
////    val normTmp: RDD[(ArrayBuffer[Double], ArrayBuffer[Double])] =
//     abLinesBefore.map(x => {
////      val xLength = x.length
////
////      var maxNormTmp: ArrayBuffer[Double] = new ArrayBuffer[Double]()
////      var minNormTmp: ArrayBuffer[Double] = new ArrayBuffer[Double]()
////
////      for(i<- 0 until(xLength)) {
////        maxNormTmp(i) = 0
////        minNormTmp(i) = Inf
////      }
//        for (i <- 0 until x.length) { //进来一个数组后逐位比较，求该位的最大最小值。
//          if (x(i) > maxNorm(i)) //0用来放最大值，1用来放最小值
//            maxNorm(i) = x(i)
//          if (x(i) < minNorm(i))
//            minNorm(i) = x(i)
//        }
//
////      (maxNormTmp, minNormTmp)
//    }
//    )
//    val norm: Array[(ArrayBuffer[Double], ArrayBuffer[Double])] = normTmp.collect()
//    maxNorm = norm(0)._1
//    minNorm = norm(0)._2
//    for(i <- 0 until(norm(0)._1.length)) {
//      brandNorm(i) = maxNorm(i)- minNorm(i)
//    }
//    println("每列最小值组成的数组为")
//    minNorm.foreach(println)
//    maxNorm.foreach(println)
//
//
//    //--------------------------------找到了每列的最大最小值------------------------------------
//    val abLines: RDD[ArrayBuffer[Double]] = abLinesBefore.map(x => {
//      var xTmp: ArrayBuffer[Double] = new ArrayBuffer[Double]()
//      for (i <- 0 until (x.length)) {
//        xTmp(i) = x(i) / brandNorm(i)
//      }
//      xTmp
//    }
//    )
    //---------------------------归一化完成------------------------------------------

    //---------------------------选距离最远的k个点做初始质心------------------------------------------------
    val centroidRandom: Array[ArrayBuffer[Double]] = new Array[ArrayBuffer[Double]](k)
    centroidRandom(0) = abLines.first()
    for (i <- 1 until (k)) {                       //k-1次求质心，每次与已有的质心的距离和做比较
      //已有簇心与所有数据作比较。K个质心需要遍历RDD k-1次
      val dec: RDD[ArrayBuffer[Double]] = sparkContext.makeRDD(centroidRandom)
      val centStep1: RDD[ArrayBuffer[Double]] = abLines.subtract(dec)     //每次过滤掉已有的质心
      val centStep2: RDD[(Double, ArrayBuffer[Double])] = centStep1.map(line => {
        var distTemp: Double = 0.0
        for (j <- 0 until i) {
          distTemp = distTemp + distance(line, centroidRandom(j)) //计算第i个质心距离现有质心的距离和
          }
        (distTemp, line)
      })
      centroidRandom(i) = centStep2.sortByKey(false).first()._2   //取第一个
    }

    //   val ClusterCentroid: Array[(Int, ArrayBuffer[Double])]
    var centroidSet : Array[(Int, ArrayBuffer[Double])] = new Array[(Int, ArrayBuffer[Double])](k)
    for (i <- 0 until k) {
      centroidSet(i) = (i,centroidRandom(i))        //转化为带簇号的初始簇心
    }
    println("----------------------------初始随机簇心为--------------------------------------")
    centroidSet.foreach(println)

//------------------------------------初始质心生成完成---------------------------------------------------------
    var centroidNew: Array[(Int, ArrayBuffer[Double])] = centroidSet
    var CentroidChanged  = true
//------------------------------------聚类-----------------------------------------------------------------

while (CentroidChanged ) {
  var disBetweenCentroidIJ = 0.0
  var countBetweenCentroid = 0
  //----------------------------------计算簇间距----------------------------------------
  //在每一次确定了新簇心后就开始计算簇间距。与和在该簇心下的簇内距求比值。
  for(i <- 0 until(centroidNew.length)) {
    if (i < centroidNew.length -1) {
      for (j <- i+1 until(centroidNew.length)) {
        val distanceIJ: Double = distance(centroidNew(i)._2,centroidNew(j)._2)
        disBetweenCentroidIJ += distanceIJ     //簇间距累加
        countBetweenCentroid += 1               //统计簇间数
      }
    }
  }
  distancebetweenAvg = disBetweenCentroidIJ / countBetweenCentroid
  //----------------------------------簇间距计算结束，开始计算簇内距---------------------------------------

  //增加了一列为了统计簇内距
//  val ClusterStep1: RDD[(Int, ArrayBuffer[Double])] =
  val ClusterStep0: RDD[((Int, ArrayBuffer[Double]), Double)] = abLines.map(line => {
    var minDistance = Inf  //初始距离无穷大
    var resultTemp: (Int, ArrayBuffer[Double]) = (-1, ArrayBuffer()) //每条结果是一个键值对的元组 tuple
    var sumDistance = 0.0 //统计总的类内距离
    for (i <- 0 until centroidNew.size) {
      if (distance(line, centroidNew(i)._2) < minDistance) {
        resultTemp = (centroidNew(i)._1, line) //(簇号，点)
        minDistance = distance(line, centroidNew(i)._2)
      }
    }
    sumDistance += minDistance;                                     //所有簇内距加起来。
    (resultTemp, sumDistance)
  })

  distanceInnerSum = ClusterStep0.map(x => x._2).sum()      //散布在各地的簇内距加起来存到本地

  val ClusterStep1: RDD[(Int, ArrayBuffer[Double])] = ClusterStep0.map( x => x._1)
  //------------------------------------聚类完成-----------------------------------------------------------------
  //----------------------------------求新质心-------------------------------------------------------------------

  val ClusterStep2: RDD[(Int, (ArrayBuffer[Double], Int))] = ClusterStep1.combineByKey( //求出了和和频次
    (v) => (v, 1),
    (acc: (ArrayBuffer[Double], Int), v) => (sumArrayBuffer(acc._1, v), acc._2 + 1), //每个分区内求和统计频次
    (acc1: (ArrayBuffer[Double], Int), acc2: (ArrayBuffer[Double], Int)) => (sumArrayBuffer(acc1._1, acc2._1), acc1._2 + acc2._2) //所有分区合并
  )
  val ClusterStep3: RDD[(Int, (ArrayBuffer[Double], Int))] = ClusterStep2.map(x => {
    val ab1: ArrayBuffer[Double] = for (elem <- x._2._1) yield elem / (x._2._2)       //按列求和除以簇内数，求出新簇心
    (x._1, (ab1,x._2._2))
  }).sortByKey()                 //为了统计每个簇内的元素数保留了后面的累加数


  val ClusterStep4: Array[(Int, (ArrayBuffer[Double], Int))] = ClusterStep3.collect()

    //新簇心:（簇号，簇心坐标）形式的数组
  val ClusterCentroid: Array[(Int, ArrayBuffer[Double])] = ClusterStep4.map(x => (x._1,x._2._1))

  println("----------------------第"+count+"次迭代，簇心为：-------------------------")
  ClusterStep4.foreach(println)
  //----------------------------------求新质心完成------------------------------------------------------
  //----------------------------------判断质心是否变化，确定是否迭代--------------------------------
  distanceCentrols = centroidDistance (centroidNew,ClusterCentroid)
  if( distanceCentrols > standardMinDistance) {
    CentroidChanged = true
    centroidNew = ClusterCentroid //作为新质心重新迭代
  }
  else
    CentroidChanged = false

  //----------------------------------平均簇内距离------------------------------------------
  for (i <- 0 until(ClusterStep4.length) ) {
    numSumAll += ClusterStep4(i)._2._2
  }
  distanceInnerAvg = distanceInnerSum / numSumAll   //平均簇内距离
  //----------------------------------簇内距/簇间距------------------------------------------
  effort = distanceInnerAvg / distancebetweenAvg //簇内距/簇间距。衡量聚类效果

  count +=1
  ClusterOut = ClusterCentroid
  println("--------------------第"+count+"次迭代，新旧簇心距离为："+distanceCentrols+"---------------------------")
  println("--------------------第"+count+"次迭代，簇内距/簇间距为："+effort+"---------------------------")

  }
  println("------------------共迭代了"+count+"次，最终簇心为：---------------------")
    ClusterOut.foreach(println)
    println("--------------------新旧簇心距离为："+distanceCentrols+"---------------------------")
    println("--------------------簇内距/簇间距为："+effort+"---------------------------")
  }
}
```
