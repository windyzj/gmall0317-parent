package com.atguigu.gmall0317.realtime.util

import java.util
import java.util.Properties

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, RangeQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder

object MyEsUtil {

    var jestClientFactory:JestClientFactory =_



   def getJestClient(): JestClient ={
        if(jestClientFactory!=null){
          jestClientFactory.getObject
        }else{
          val properties: Properties = PropertiesUtil.load("config.properties")
          val host=properties.getProperty("elasticsearch.host")
          val port=properties.getProperty("elasticsearch.port")
          jestClientFactory=new JestClientFactory
          jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://"+host+":"+port)
            .maxTotalConnection(4)
            .multiThreaded(true).build())

          jestClientFactory.getObject
        }
   }

  //batch
  def saveDocBulk(docList:List[(Any,String)],indexName:String): Unit ={
    val jestClient: JestClient = getJestClient
    val bulkBuilder = new Bulk.Builder
    bulkBuilder.defaultIndex(indexName).defaultType("_doc")
    for ((doc, id) <- docList ) {
      val index: Index = new Index.Builder(doc).id(id).build()
      bulkBuilder.addAction(index)
    }
    val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulkBuilder.build()).getItems
    items
    println("共提交："+items.size()+"条数据")
    import collection.JavaConverters._
    for (item <- items.asScala ) {
        if(item.errorReason!=null&&item.errorReason.size>0 ){
          println("es error reason:"+item.errorReason)
          throw new RuntimeException("")
        }
    }

    jestClient.close()
  }



  def  saveDoc(doc:Any,indexName:String): Unit ={

    val jestClient: JestClient = getJestClient

    val index = new Index.Builder(doc).index(indexName).`type`("_doc").build()
    jestClient.execute(index)

    jestClient.close()


  }

  def searchDoc(indexName:String): Unit ={
    val jestClient: JestClient = getJestClient()
   //val query="{""query\": {\n    \"bool\": {\n      \"filter\": {\n         \"range\": {\n           \"doubanScore\": {\n             \"gte\": 6,\n             \"lte\": 10\n           }\n         }\n      },\n      \"must\": [\n        {\"match\": {\n          \"name\": \"red\"\n        }}\n      ]\n      \n    }\n  }\n  \n}"
    // es 提供了 查询条件的封装工具
    val sourceBuilder = new SearchSourceBuilder
    val bool:BoolQueryBuilder = new BoolQueryBuilder()
    bool.filter(new RangeQueryBuilder("doubanScore").gte(6).lte(10))
    bool.must(new MatchQueryBuilder("name","red"))
    sourceBuilder.query(bool)
    println(sourceBuilder.toString)

    val search: Search = new Search.Builder(sourceBuilder.toString).addIndex(indexName).addType("movie").build()
    val searchResult: SearchResult = jestClient.execute(search)
    val hits: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = searchResult.getHits(classOf[util.Map[String,Any]])
    import  collection.JavaConverters._
    for (hit <- hits.asScala ) {
      val source: util.Map[String, Any] = hit.source
      println(source.get("name"))
      println(source.get("doubanScore"))
    }
    jestClient.close()

  }


  def main(args: Array[String]): Unit = {
   // saveDoc(Movie("0202","乘风破浪"),"movie_test0317_20200829")
    searchDoc("movie_index0317")
  }





  case class  Movie(id:String,name:String)


}
