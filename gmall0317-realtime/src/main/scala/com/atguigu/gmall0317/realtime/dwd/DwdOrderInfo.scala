package com.atguigu.gmall0317.realtime.dwd

import java.lang

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0317.realtime.bean.OrderInfo
import com.atguigu.gmall0317.realtime.util.{MyEsUtil, MyKafkaSender, MyKafkaUtil, OffsetManager, PhoenixUtil}
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
      val orderInfo: OrderInfo = JSON.parseObject(jsonString,classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")

      orderInfo.create_date=createTimeArr(0)
      orderInfo.create_hour=createTimeArr(1).split(":")(0)

      orderInfo
    }

//    orderInfoDstream.map{orderInfo=>
//      val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select * from gmall0317_province_info where id='"+orderInfo.province_id+"'")
//      val provinceJsonObj: JSONObject = provinceJsonObjList(0)
//      orderInfo.province_3166_2_code=provinceJsonObj.getString("province_3166_2_code")
//   //   orderInfo.province_id ==> orderInfo.province_3166_2_code
//      orderInfo
//    }

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

    //////////////////用户信息关联//////////////////////////


    val orderInfoWithDimDstream: DStream[OrderInfo] = orderInfoWithProvinceDstream.mapPartitions { orderInfoItr =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      val userIdList: List[Long] = orderInfoList.map(_.user_id)
      val sql = "select id ,user_level ,  birthday  , gender  , age_group  , gender_name from gmall0317_user_info ui where  ui.id in ('" + userIdList.mkString("','") + "')";
      val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
      val userJsonMap: Map[lang.Long, JSONObject] = userJsonObjList.map(userJsonObj => (userJsonObj.getLong("ID"), userJsonObj)).toMap
      for (orderInfo <- orderInfoList) {
        val userJsonObj: JSONObject = userJsonMap.getOrElse(orderInfo.user_id, null)
        if (userJsonObj != null) {
          orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
          orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
        }
      }

      orderInfoList.toIterator
    }















//    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoWithProvinceDstream.mapPartitions { orderInfoItr =>
//      val orderList: List[OrderInfo] = orderInfoItr.toList
//      if(orderList.size>0) {
//        val userIdList: List[Long] = orderList.map(_.user_id)
//        val sql = "select id ,user_level ,  birthday  , gender  , age_group  , gender_name from gmall0105_user_info where id in ('" + userIdList.mkString("','") + "')"
//        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
//        val userJsonObjMap: Map[Long, JSONObject] = userJsonObjList.map(userJsonObj => (userJsonObj.getLongValue("ID"), userJsonObj)).toMap
//        for (orderInfo <- orderList) {
//          val userJsonObj: JSONObject = userJsonObjMap.getOrElse(orderInfo.user_id, null)
//          orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
//          orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
//        }
//      }
//      orderList.toIterator
//    }



    orderInfoWithDimDstream.foreachRDD { rdd =>
      rdd.foreachPartition { orderInfoItr =>
        val orderInfolist: List[OrderInfo] = orderInfoItr.toList
        if (orderInfolist != null && orderInfolist.size > 0) {
          val create_date: String = orderInfolist(0).create_date

          val orderInfoWithIdList: List[(OrderInfo, String)] = orderInfolist.map(orderInfo => (orderInfo, orderInfo.id.toString))

          val indexName = "gmall0317_order_info_" + create_date
//          MyEsUtil.saveDocBulk(orderInfoWithIdList, indexName)
        }
        for (orderInfo <- orderInfolist ) {
          MyKafkaSender.send("DWD_ORDER_INFO",JSON.toJSONString(orderInfo,new SerializeConfig(true)))  //fastjson不能直接把case class 转为jsonstring
        }
      }

     OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }



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
