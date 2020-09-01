package com.atguigu.gmall0317.realtime.bean

case class ProvinceInfo(id:String,
                        name:String,
                        area_code:String,// 行政区位码 阿里云datav quickbi
                        iso_code:String, // superset用
                        iso_3166_2:String // kibana用
                       ) {

}

