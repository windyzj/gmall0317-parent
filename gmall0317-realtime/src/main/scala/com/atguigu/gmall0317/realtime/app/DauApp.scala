package com.atguigu.gmall0317.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0317.realtime.bean.DauInfo
import com.atguigu.gmall0317.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

// dau= daily active user     uv = user view , pv = page view
object DauApp {

  def main(args: Array[String]): Unit = {
    //1、尝试读取redis中的偏移量  OffsetManager.getOffset

    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "GMALL0317_STARTUP"
    val groupId = "dau_app_group"
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

    //4、计算（调整结构、去重、关联、计算、聚合....）

    //inputDstream.map(_.value()).print(100)

    //前置处理 1 结构化  2 日期 3 小时
    val jsonObjDstream: DStream[JSONObject] = inputWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      val ts: lang.Long = jsonObj.getLong("ts")
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateTimeStr: String = dateFormat.format(new Date(ts))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      val dt: String = dateTimeArr(0)
      val hr: String = dateTimeArr(1)
      jsonObj.put("dt", dt)
      jsonObj.put("hr", hr)
      jsonObj
    }
    val filteredDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions { jsonObjItr =>
    val jedis: Jedis = RedisUtil.getJedisClient // 1 连接池
    val jsonList: List[JSONObject] = jsonObjItr.toList
    println("过滤前："+jsonList.size )
    val filteredList = new ListBuffer[JSONObject]
      for (jsonObj <- jsonList) {
        val dauKey = "dau:" + jsonObj.get("dt")
        val mid = jsonObj.getJSONObject("common").getString("mid")
        val ts: Long = new Date().getTime
        val ifNonExists: lang.Long = jedis.zadd(dauKey, ts,mid)
        if (ifNonExists == 1) {
          filteredList += jsonObj
        }
      }
      jedis.close()
      println("过滤后："+filteredList.size )
      filteredList.toIterator
    }


/*    // 数据中 凡是今天启动过的用户(mid)进行记录，同时进行筛选如果之前访问过 则过滤掉
    val jsonObjFilteredDStream: DStream[JSONObject] = jsonObjDstream.filter { jsonObj =>
      // Redis  type？ string set hash zset list   key?     value
      // string   key?  dau:[date]:mid:[mid]   value 1/0    api?  setnx  如果有需求：查询今天所有的mid  keys dau:[date]:mid:*  O(N)     可以分摊到多台机器中
      // set     key ?  dau:[date]   value? [mid]    api?  sadd   如果有需求：查询今天所有的mid  smembers key O(1)  无法通过集群方式分摊数据和qps
      // hash   key ? dau:[date]   value? [mid] :1    api? hash 可以但没必要
      // zset   key?   dau:[date]   value? [mid] score  api? zadd  //可以但没必要吗？
      val jedis: Jedis = RedisUtil.getJedisClient // 1 连接池
      val dauKey = "dau:" + jsonObj.get("dt")
      val mid = jsonObj.getJSONObject("common").getString("mid")
      val ifNonExists: lang.Long = jedis.sadd(dauKey, mid)
      jedis.close()
      if (ifNonExists == 1) {
        true
      } else {
        false
      }

    }*/

    filteredDstream.cache()

    filteredDstream.print(100)

    filteredDstream.foreachRDD{rdd=>
      //5、计算结果的保存

      ///保存操作  问题：1 不幂等 2 每条数据提交 浪费io
//      rdd.foreach{jsonObj=>
//        val commonObj: JSONObject = jsonObj.getJSONObject("common")
//        val dauInfo = DauInfo(commonObj.getString("mid"), commonObj.getString("uid"), commonObj.getString("ar"), commonObj.getString("ch")
//          , commonObj.getString("vc"), jsonObj.getString("dt"), jsonObj.getString("hr"), "00", jsonObj.getLong("ts"))
//
//         MyEsUtil.saveDoc(dauInfo,"gmall_dau_info_0317_"+jsonObj.getString("dt"))
//    }

      ///保存操作  问题：1  幂等 2  批量操作
      rdd.foreachPartition{jsonObjItr=>
        val jsonObjList: List[JSONObject] = jsonObjItr.toList
        if(jsonObjList.size >0){
            val docList: List[(DauInfo, String)] = jsonObjList.map { jsonObj =>
              val commonObj: JSONObject = jsonObj.getJSONObject("common")
              val dauInfo = DauInfo(commonObj.getString("mid"), commonObj.getString("uid"), commonObj.getString("ar"), commonObj.getString("ch")
                , commonObj.getString("vc"), jsonObj.getString("dt"), jsonObj.getString("hr"), "00", jsonObj.getLong("ts"))
              (dauInfo, dauInfo.mid)
            }
            val dt: String = docList(0)._1.dt
            MyEsUtil.saveDocBulk( docList  ,"gmall_dau_info_0317_"+dt)
        }
      }


      //6、  把偏移量结束位置更新到redis/mysql
      //提交偏移量
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()


  }

}
