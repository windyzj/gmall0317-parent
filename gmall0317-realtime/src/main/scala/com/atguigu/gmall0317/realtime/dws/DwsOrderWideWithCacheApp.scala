package com.atguigu.gmall0317.realtime.dws

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0317.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0317.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DwsOrderWideWithCacheApp {

  def main(args: Array[String]): Unit = {
    // 加载流 //手动偏移量
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dws_order_wide_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dws_order_wide_group"

    val topicOrderInfo = "DWD_ORDER_INFO";
    val topicOrderDetail = "DWD_ORDER_DETAIL"

    //1   从redis中读取偏移量   （启动执行一次）
    val offsetMapForKafkaOrderInfo: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderInfo, groupId)
    val offsetMapForKafkaOrderDetail: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderDetail, groupId)

    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    var recordInputDstreamOrderInfo: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafkaOrderInfo != null && offsetMapForKafkaOrderInfo.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      recordInputDstreamOrderInfo = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, offsetMapForKafkaOrderInfo, groupId)
    } else {
      recordInputDstreamOrderInfo = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, groupId)
    }

    var recordInputDstreamOrderDetail: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafkaOrderDetail != null && offsetMapForKafkaOrderDetail.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      recordInputDstreamOrderDetail = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, offsetMapForKafkaOrderDetail, groupId)
    } else {
      recordInputDstreamOrderDetail = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, groupId)
    }


    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRangesOrderDetail: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstreamOrderDetail: DStream[ConsumerRecord[String, String]] = recordInputDstreamOrderDetail.transform { rdd => //周期性在driver中执行
      offsetRangesOrderDetail = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    var offsetRangesOrderInfo: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstreamOrderInfo: DStream[ConsumerRecord[String, String]] = recordInputDstreamOrderInfo.transform { rdd => //周期性在driver中执行
      offsetRangesOrderInfo = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }



    // 1 提取数据 2 分topic
    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstreamOrderDetail.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }
    //orderDetailDstream.print(100)

    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstreamOrderInfo.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      orderInfo
    }
    //orderInfoDstream.print(100)

    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

    val fullJoinedDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)
    //      1  如果通过join能够直接join上     把配对的元组变为orderWide
    //      OrderInfo
    //      2.1   把自己写入缓存
    //        2.2   查询缓存 看看有没有能跟自己匹配的OrderDetail
    //        把配对的变为orderWide
    //
    //      OrderDetail
    //      3.1 把自己写入缓存
    //        3.2 查询缓存  看看有没有能跟自己匹配的OrderInfo
    //        把配对的元组变为orderWide
    val orderWideDstream: DStream[OrderWide] = fullJoinedDstream.flatMap { case (orderId, (orderInfoOpt, orderDetailOpt)) =>
      val orderWideList = new ListBuffer[OrderWide]
      val jedis: Jedis = RedisUtil.getJedisClient
      if (orderInfoOpt != None) {
        val orderInfo: OrderInfo = orderInfoOpt.get
        if (orderDetailOpt != None) {
          //      1  如果通过join能够直接join上     把配对的元组变为orderWide
          val orderDetail: OrderDetail = orderDetailOpt.get
          orderWideList.append(new OrderWide(orderInfo, orderDetail))
        }
        //      OrderInfo
        //      2.1   把自己写入缓存    作用? 让从表查   type ? string        key ?  orderjoin:orderInfo:[order_id]  value? orderInfoJson     api? setex
        val orderInfoKey = "orderjoin:orderInfo:" + orderInfo.id
        val orderInfoJson = JSON.toJSONString(orderInfo, new SerializeConfig(true))
        jedis.setex(orderInfoKey, 600, orderInfoJson)
        //        2.2   查询缓存 看看有没有能跟自己匹配的OrderDetail
        //        把配对的变为orderWide
        val orderDetailKey = "orderjoin:orderDetail:" + orderInfo.id
        val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailKey)
        if (orderDetailJsonSet != null && orderDetailJsonSet.size() > 0) {
          import collection.JavaConverters._
          for (orderDetailJson <- orderDetailJsonSet.asScala) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            orderWideList += new OrderWide(orderInfo, orderDetail)
          }
        }
      } else {
        //      OrderDetail
        //      3.1 把自己写入缓存  作用 让主表查 type ?   set    key   ?  orderjoin:orderDetail:[order_id]   value?    orderDetailJson  api?  sadd
        val orderDetail: OrderDetail = orderDetailOpt.get
        val orderDetailKey = "orderjoin:orderDetail:" + orderDetail.order_id
        val orderDetailJson = JSON.toJSONString(orderDetail, new SerializeConfig(true))
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 600)
        //        3.2 查询缓存  看看有没有能跟自己匹配的OrderInfo
        //        把配对的元组变为orderWide
        val orderInfoKey = "orderjoin:orderInfo:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderInfoJson.size > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          orderWideList += new OrderWide(orderInfo, orderDetail)
        }
      }
      jedis.close()
      orderWideList


    }

    orderWideDstream.print(1000)
    ssc.start()
    ssc.awaitTermination()


    // 保存到clickhouse 里
  }

}
