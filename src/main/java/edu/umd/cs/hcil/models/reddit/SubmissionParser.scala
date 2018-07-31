package edu.umd.cs.hcil.models.reddit

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  * Created by cbuntain on 7/20/17.
  */
object SubmissionParser {

  // JSON default format
  implicit val formats = DefaultFormats

  def parseJson(submissionJson : String): SubmissionModel = {
    try {
      val jsonNode = parse(submissionJson, false)
      jsonNode.extract[SubmissionModel]
    } catch {
      case e : Exception => {
        null
      }
    }
  }

  case class SubmissionModel (
                               author : String,
                               created : Option[Long],
                               created_utc : Option[Long],
                               domain : String,
                               downs : Option[Long],
                               ups : Option[Long],
                               id : String,
                               is_self : Boolean,
                               num_comments : Long,
                               permalink : String,
                               score : Long,
                               selftext : String,
                               subreddit : Option[String],
                               subreddit_id : Option[String],
                               thumbnail : String,
                               title : String,
                               url : String

                             ) extends RedditModel {

    def text : Option[String] = {
      Some(title + (if ( is_self ) { "," + selftext } else { "" }))
    }
  }

}
