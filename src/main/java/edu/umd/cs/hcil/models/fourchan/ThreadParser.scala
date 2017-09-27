package edu.umd.cs.hcil.models.fourchan

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.jsoup.Jsoup

/**
  * Created by cbuntain on 9/13/17.
  */
object ThreadParser {


  // JSON default format
  implicit val formats = DefaultFormats

  def parseJson(threadJson : String): ThreadModel = {
    val jsonNode = parse(threadJson , false)
    try {
      jsonNode.extract[ThreadModel]
    } catch {
      case e : Exception => null
    }
  }

  def getPlainText(post : PostModel) : String = {
    return Jsoup.parse(post.com).text()
  }

  case class ThreadModel (
                         posts : List[PostModel]
                         )

  case class PostModel (
                         no: Long,
                         com: String,
                         now: String,
                         name: String,
                         time: Long,
                         resto: Long,
                         country: String,
                         country_name: String
                       // The following are only in the first post, so we skip
//                         h: Int,
//                         w: Int,
//                         ext: String,
//                         md5: String,
//                         tn_h: Int,
//                         tn_w: Int,
//                         fsize: Long,
//                         filename: String,
                       )

}
