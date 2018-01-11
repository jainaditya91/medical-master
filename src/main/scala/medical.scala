/**
  * Created by Aditya Jain on 14-06-2017.
  */
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object medical {

  def main(args:Array[String]): Unit={
    val conf = new SparkConf().setMaster("local").setAppName("medical")
    val sc = new SparkContext(conf)
    val inputBaseDir = args(0)
    val outputpath = args(1)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    if(!fs.exists(new Path(inputBaseDir))) {
      println("base directory doesnt exist")
      return
    }



    val op = new Path(outputpath )
    if(fs.exists(op)){
      fs.delete(op,true)
    }


    val orders = sc.textFile(inputBaseDir + "/medical_disease")
    val data = orders.mapPartitionsWithIndex((idx, itr) => if(idx == 0) itr.drop(1) else itr)
    val orderFiltered = data.filter(rec=> rec.split(",")(3)=="COMPLETE"||rec.split("")(3)=="CLOSED")
    val ordersMap = orderFiltered.map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))
    val order_item = sc.textFile(inputBaseDir + "/medical_prescribed")
    val data1 = order_item.mapPartitionsWithIndex((idx, itr) => if(idx == 0) itr.drop(1) else itr)
    val order_itemMap = data1.map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toDouble))
    val ordersjoin = ordersMap.join(order_itemMap)
    val ordersJoinMap = ordersjoin.map(rec => rec._2)

    val ordersJoinMapABK = ordersJoinMap.aggregateByKey((0.0,0))(
      (intAgg : (Double,Int), intVal :Double) => (intAgg._1 + intVal, intAgg._2+1),
      //(60000,60)(40000,40)
      (totAgg: (Double,Int), totVal: (Double,Int))=>
        (totAgg._1 + totVal._1, totAgg._2 + totVal._2)
    )

    ordersJoinMapABK.saveAsTextFile(outputpath)
  }

}
