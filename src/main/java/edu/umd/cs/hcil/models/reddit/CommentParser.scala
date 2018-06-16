package edu.umd.cs.hcil.models.reddit

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  * Created by cbuntain on 5/22/18.
  */
object CommentParser {

  // JSON default format
  implicit val formats = DefaultFormats

  def parseJson(commentJson : String): CommentModel = {
    val jsonNode = parse(commentJson, false)
    try {
      jsonNode.extract[CommentModel]
    } catch {
      case e : Exception => {
        if ( true ) {
          println(e)
        }
        null
      }
    }
  }

  case class CommentModel (
                               author : String,
                               body : String,
                               controversiality : Long,
                               created : Option[Long],
                               created_utc : Option[Long],
                               distinguished : Option[String],
                               gilded : Long,
                               id : String,
                               link_id : String,
                               parent_id : String,
                               retrieved_on : Long,
                               score : Long,
                               subreddit : Option[String],
                               subreddit_id : Option[String]

                             ) extends RedditModel
}
