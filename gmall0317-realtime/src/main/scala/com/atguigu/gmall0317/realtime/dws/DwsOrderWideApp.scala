package com.atguigu.gmall0317.realtime.dws

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0317.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0317.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import collection.JavaConverters._
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
    val orderInfoWindowDstream: DStream[OrderInfo] = orderInfoDstream.window(Seconds(10), Seconds(5))
    val orderDetailWindowDstream: DStream[OrderDetail] = orderDetailDstream.window(Seconds(10), Seconds(5))


    val orderInfoWindowWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWindowDstream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWindowWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailWindowDstream.map(orderDetail => (orderDetail.order_id, orderDetail))

    val orderTupleDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWindowWithKeyDstream.join(orderDetailWindowWithKeyDstream)

    val orderWideDstream: DStream[OrderWide] = orderTupleDstream.map { case (orderId, (orderInfo, orderDetail)) => new OrderWide(orderInfo, orderDetail) }



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

    //orderWideDstream.print(1000)


    val orderWideWithFinalAmountDstream: DStream[OrderWide] = filteredOrderWideDstream.map { orderWide =>

      //1  要先进行是否是最后一笔的判断
      //   判断公式： 该笔订单明细的应付金额  ==  订单的应付总额-累计的其他明细的应付金额
      //						                          ==   original_total_amount -  Σ (sku_price*sku_num)
      val originalTotalAmount = BigDecimal(orderWide.original_total_amount) //订单应付总额
    val originDetail: BigDecimal = BigDecimal(orderWide.sku_price * orderWide.sku_num) //订单单笔明细应付
    val finalTotalAmount: BigDecimal = BigDecimal(orderWide.final_total_amount) //订单实付总额
    var accOriginDetailTotal = BigDecimal(0) //累计总应付值
    var accFinalDetailTotal = BigDecimal(0) //累计总实付值
    val finalAmountKey = "final_amount:" + orderWide.order_id //累计总实付key
    val originalAmountKey = "origin_amount:" + orderWide.order_id
      var finalAmountMap: util.Map[String, String] = null
      // Redis中如何保存  累计的其他明细的应付金额
      //              type？  hash     key ?    origin_amount:[orderId]  field ? order_detail_id    value ? sku_price*sku_num    api? 读  hgetall 写  hset
      //  累计 存总值？ 还是存明细？
      val jedis: Jedis = RedisUtil.getJedisClient
      val orginalAmountMap: util.Map[String, String] = jedis.hgetAll(originalAmountKey)
      if (orginalAmountMap != null && orginalAmountMap.size() > 0) {
        orginalAmountMap.put(orderWide.order_detail_id.toString, originDetail.toString()) //防止有重发的数据做幂等性处理
        //  累计总应付值 ==？ 订单总应付
        for ((orderDetailId, orderOriginDetail) <- orginalAmountMap.asScala) {
          accOriginDetailTotal += BigDecimal(orderOriginDetail)
        }
      }
      if (originalTotalAmount == accOriginDetailTotal) {
        //2 如果是最后一笔  减法
        //  减法公式：			   final_detail_amount = 订单实付总金额 - 累计的其他明细的实付金额
        //											                = final_total_amount - Σ final_detail_amount（other)
        // Redis中如何保存  累计的其他明细的实付金额
        //              type？  hash     key ?    final_amount:[orderId]  field ? order_detail_id    value ?  final_detail_amount    api? 读  hgetall 写  hset
        //  累计 存明细

        finalAmountMap = jedis.hgetAll(finalAmountKey)
        if (finalAmountMap != null && finalAmountMap.size() > 0) {
          for ((orderDetailOtherId, finalDetailAmount) <- finalAmountMap.asScala) {
            if (orderDetailOtherId != orderWide.order_detail_id.toString) { //如果极端情况出现重算 ，而redis中有残留的历史明细  要剔除掉   ?
              accFinalDetailTotal += BigDecimal(finalDetailAmount)
            }
          }
        }
        orderWide.final_detail_amount = (finalTotalAmount - accFinalDetailTotal).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue()
      } else {
        // 3  如果不是最后一笔  乘除法
        // 乘除法公式 ：
        // 商品的实付分摊金额=订单实付总金额 * 商品的应付金额/订单的应付总额
        //final_detail_amount  = final_total_amount * (sku_price* sku_num) / original_total_amount
        val finalDetailAmount: BigDecimal = finalTotalAmount * originDetail / originalTotalAmount
        orderWide.final_detail_amount = finalDetailAmount.setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue()
      }
      if (orderWide.final_detail_amount == BigDecimal.valueOf(0L)) {
        println(orderWide)
      }
      //4 把计算结构写入redis 以备后续明细计算
      //把实付明细写入
      jedis.hset(finalAmountKey, orderWide.order_detail_id.toString, orderWide.final_detail_amount.toString)
      //把应付明细写入
      jedis.hset(originalAmountKey, orderWide.order_detail_id.toString, originDetail.toString())
      jedis.close()

      orderWide
    }
   //checkpoint     persist
    orderWideWithFinalAmountDstream.cache()
    orderWideWithFinalAmountDstream.print(1000)


    val sparkSession = SparkSession.builder()
      .appName("order_detail_wide_spark_app")
      .getOrCreate()
    import sparkSession.implicits._
    // 保存到clickhouse 里
    orderWideWithFinalAmountDstream.foreachRDD { rdd =>
      val df: DataFrame = rdd.toDF()
      df.write.mode(SaveMode.Append)
        .option("batchsize", "100")
        .option("isolationLevel", "NONE") // 设置事务
        .option("numPartitions", "4") // 设置并发
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://hdp1:8123/gmall0317", "order_wide_0317", new Properties())

      OffsetManager.saveOffset(topicOrderInfo, groupId, offsetRangesOrderInfo)
      OffsetManager.saveOffset(topicOrderDetail, groupId, offsetRangesOrderDetail)
    }

    ssc.start()
    ssc.awaitTermination()



  }

}
