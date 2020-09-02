package com.atguigu.gmall0317.realtime.dws

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0317.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0317.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DwsOrderWideApp {

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
   // orderDetailDstream.print(100)

    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstreamOrderInfo.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      orderInfo
    }
   // orderInfoDstream.print(100)


//    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
//    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
//    orderInfoWithKeyDstream.join(orderDetailWithKeyDstream).print(1000)


    //1
    val orderInfoWindowDstream: DStream[OrderInfo] = orderInfoDstream.window(Seconds(10),Seconds(5))
    val orderDetailWindowDstream: DStream[OrderDetail] = orderDetailDstream.window(Seconds(10),Seconds(5))


    val orderInfoWindowWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWindowDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    val orderDetailWindowWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailWindowDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

    val orderTupleDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWindowWithKeyDstream.join(orderDetailWindowWithKeyDstream)

    val orderWideDstream: DStream[OrderWide] = orderTupleDstream.map{case(orderId,(orderInfo,orderDetail))=> new OrderWide(orderInfo,orderDetail)  }



    // 去重
    //redis    zset     key  orderwide:[2020-09-02]   ?    value    order_detail.id  ?  ts

    val filteredOrderWideDstream: DStream[OrderWide] = orderWideDstream.mapPartitions { orderWideItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val orderWideList: List[OrderWide] = orderWideItr.toList
      val filteredOrderWideList = new ListBuffer[OrderWide]
      if (orderWideList != null && orderWideList.size > 0) {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        for (orderWide <- orderWideList) {
          val dt: String = orderWide.dt
          val key = "orderwide:" + dt
          val ts: Long = dateFormat.parse(orderWide.create_time).getTime
          val isNonExists: Long = jedis.zadd(key, ts, orderWide.order_detail_id.toString)
          if (isNonExists == 1L) {
            filteredOrderWideList.append(orderWide)
          }
        }
      }
      jedis.close()
      filteredOrderWideList.toIterator
    }

    filteredOrderWideDstream.print(1000)
    ssc.start()
    ssc.awaitTermination()


    // 保存到clickhouse 里
  }

}
