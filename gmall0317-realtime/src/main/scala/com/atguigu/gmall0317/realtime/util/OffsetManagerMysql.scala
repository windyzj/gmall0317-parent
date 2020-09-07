package com.atguigu.gmall0317.realtime.util

import java.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManagerMysql {


  def getOffset(topic:String,consumerGroupId:String): Map[TopicPartition,Long] ={
      var sql="select partition_id,topic_offset from offset_0317 where topic='"+topic+"' and group_id='"+consumerGroupId+"'"
      val offsetJsonObjList: List[JSONObject] = MysqlUtil.queryList(sql)
     if(offsetJsonObjList!=null&&offsetJsonObjList.size>0){
        val offsetMap: Map[TopicPartition, Long] = offsetJsonObjList.map { jsonobj =>
        val partitionId: Integer = jsonobj.getInteger("partition_id")
        val offset: Long = jsonobj.getLong("topic_offset")
        (new TopicPartition(topic, partitionId), offset)
      }.toMap
      offsetMap
    }else{
      println("没有找到已存在的偏移量！")
      null
    }
  }

  //写入偏移量
  def saveOffset(topic:String,groupId:String,  offsetRanges: Array[OffsetRange]): Unit ={

  import com.alibaba.fastjson.JSONObject

}

}
