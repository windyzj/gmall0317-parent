package com.atguigu.gmall0317.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall0317.realtime.util.{MyKafkaSender, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object OdsGmallCanal {

// 语义 ： 1 精确一次消费 2 至少一次 3 最多一次
  /**
    * 主要任务： 1  真正的数据 筛选处理  其他没有的字段不要
    *           2   根据表划分主题，主题分流
    *           3   输出kafka
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_gmall_canal_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_DB_GMALL0317_C"
    val groupId = "ods_gmall_canal_group"
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

    val jsonObjDstream: DStream[JSONObject] = inputWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      JSON.parseObject(jsonString)
    }

    // 数据筛选 ， 分流
    jsonObjDstream.foreachRDD{rdd=>
      rdd.foreach{jsonObj=>
        val jSONArray: JSONArray = jsonObj.getJSONArray("data")
        val table: String = jsonObj.getString("table")
        val topic="ODS_T_"+table.toUpperCase
        for(i<- 0 to jSONArray.size()-1){
          val dataJson: String = jSONArray.getString(i)
          MyKafkaSender.send(topic,dataJson)
        }
      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }



}
