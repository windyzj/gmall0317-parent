package com.atguigu.gmall0317.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManager {


  def getOffset(topic:String,consumerGroupId:String): Map[TopicPartition,Long] ={
    val jedis: Jedis = RedisUtil.getJedisClient
     //redis中 偏移量的存储方式  type?    hash     key? [topic]:[groupId]     field ? partitionid  value? offset  api?  写 set mset  读：hgetall
    // 每个topic --> 多个消费者组 --> 多个分区 -->每个分区有自己的偏移
    val offsetKey=topic+":"+consumerGroupId
    val offsetJavaMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
     //  list[(xx,xx)] 不可变的map[xx,xx]
    import collection.JavaConverters._
    if(offsetJavaMap!=null&&offsetJavaMap.size>0){

      val offsetList: List[(String, String)] = offsetJavaMap.asScala.toList
      val offsetListTp: List[(TopicPartition, Long)] = offsetList.map { case (partitionId, offset) =>
        println("加载-->分区："+partitionId+"--偏移量："+offset)
        (new TopicPartition(topic, partitionId.toInt), offset.toLong)
      }
      val map: Map[TopicPartition, Long] = offsetListTp.toMap
      map
    }else{
      println("没有找到已存在的偏移量！")
      null
    }
  }

  //写入偏移量
  def saveOffset(topic:String,groupId:String,  offsetRanges: Array[OffsetRange]): Unit ={
    //redis中 偏移量的存储方式  type?    hash     key? [topic]:[groupId]     field ? partitionid  value? offset  api?  写 hset hmset  读：hgetall
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey=topic+":"+groupId
    val offsetMap = new util.HashMap[String,String]()
    for (offsetRange <- offsetRanges ) {
      val partition: Int = offsetRange.partition
      val offset: Long = offsetRange.untilOffset
      offsetMap.put(partition.toString,offset.toString)
      println("写入偏移量 -->分区："+partition+"--偏移量："+offset)
    }
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()


  }

}
