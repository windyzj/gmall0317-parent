package com.atguigu.gmall0317.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0317.realtime.bean.OrderInfo
import com.atguigu.gmall0317.realtime.util.{MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object DwdOrderInfo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_order_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_T_ORDER_INFO"
    val groupId = "dwd_order_info_group"
    var inputDstream: InputDStream[ConsumerRecord[String, String]]=null
    val offsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId )

    //2、 把偏移量交给kafka ，让kafka按照偏移量的位置读取数据流
    if(offsetMap!=null){
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc,offsetMap, groupId)
    }else{
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc,  groupId)
    }

    //3、  获得偏移量的结束位置
    //从流中rdd 获得偏移量的结束点 数组
    var offsetRanges: Array[OffsetRange]=null
    val inputWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderInfoDstream: DStream[OrderInfo] = inputWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      JSON.parseObject(jsonString,classOf[OrderInfo])
    }

    orderInfoDstream.map{orderInfo=>
      val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from gmall0317_province_info where id='"+orderInfo.province_id+"'")
      val provinceJsonObj: JSONObject = provinceJsonObjList(0)
      orderInfo.province_3166_2_code=provinceJsonObj.getString("province_3166_2_code")
   //   orderInfo.province_id ==> orderInfo.province_3166_2_code
      orderInfo
    }

//    // driver中执行 启动时执行 只执行一次
//    val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from gmall0317_province_info  ")
//    val provinceInfoMap: Map[String, JSONObject] = provinceJsonObjList.map(jsonObject=>(jsonObject.getString("id"),jsonObject)).toMap
//    val provinceInfoMapBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceInfoMap)

    val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoDstream.transform { rdd =>

      // driver中执行 周期性执行
      val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from gmall0317_province_info  ")
      val provinceInfoMap: Map[String, JSONObject] = provinceJsonObjList.map(jsonObject => (jsonObject.getString("ID"), jsonObject)).toMap
      val provinceInfoMapBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceInfoMap)
      val orderInfoWithProvinceRdd: RDD[OrderInfo] = rdd.map { orderInfo =>
        //ex
        val provinceMap: Map[String, JSONObject] = provinceInfoMapBC.value
        val provinceJsonObj: JSONObject = provinceMap.getOrElse(orderInfo.province_id.toString, null)
        if(provinceJsonObj!=null){
          orderInfo.province_3166_2_code = provinceJsonObj.getString("ISO_3166_2")
          orderInfo.province_name = provinceJsonObj.getString("NAME")
          orderInfo.province_area_code = provinceJsonObj.getString("AREA_CODE")
        }

        println(11111)
        orderInfo
      }
      orderInfoWithProvinceRdd
    }
    orderInfoWithProvinceDstream.print(100)






//    orderInfoDstream.map { orderInfo  =>
//      val provinceMap: Map[String, JSONObject] = provinceInfoMapBC.value
//      val provinceJsonObj: JSONObject = provinceMap.getOrElse( orderInfo.province_id.toString,null )
//      orderInfo.province_3166_2_code=provinceJsonObj.getString("province_3166_2_code")
//      println(11111)
//      orderInfo
//    }
    println(222222222) // driver中执行 启动时执行 只执行一次

   ssc.start()
   ssc.awaitTermination()



  }

}
