package com.atguigu.gmall0317.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0317.realtime.bean.{BaseCategory3, OrderWide}
import com.atguigu.gmall0317.realtime.util.{MyKafkaUtil, OffsetManager, OffsetManagerMysql}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

object AdsTrademarkApp {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ads_trademark_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "DWS_ORDER_WIDE";
    val groupId = "ads_trademark_group"


    /////////////////////  偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManagerMysql.getOffset(topic, groupId)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    val orderWideDstream: DStream[OrderWide] = inputGetOffsetDstream.map { record =>
      val orderWide: OrderWide = JSON.parseObject(record.value(), classOf[OrderWide])
      orderWide

    }

    orderWideDstream.print(1000)

    //维度聚合
    val trademarkSumDstream: DStream[(String, Double)] = orderWideDstream.map(orderWide=>(orderWide.tm_name,orderWide.final_detail_amount)).reduceByKey(_+_)


    trademarkSumDstream.foreachRDD{rdd=>
      val trademarkTupleArr: Array[(String, Double)] = rdd.collect()

      DBs.setup()
      DB.localTx(implicit session => {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val statTime: String = dateFormat.format(new Date())
        //数据保存 表
        for ((trademarkName, orderAmount) <- trademarkTupleArr) {
            SQL( " insert into  trademark_order_amount_stat values(?,?,?)").bind(statTime,trademarkName,orderAmount).update().apply()
        }
        //偏移量的保存
        for (offsetRange <- offsetRanges ) {
          SQL( " replace into  offset_0317 values(?,?,?,?)").bind(groupId,topic,offsetRange.partition,offsetRange.untilOffset).update().apply()
        }

      })
    }

    ssc.start()
    ssc.awaitTermination()



  }

}
