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
    val jsonNode = parse(submissionJson, false)
    try {
      jsonNode.extract[SubmissionModel]
    } catch {
      case e : Exception => {
        if ( true ) {
          println(e)
        }
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
                               subreddit : String,
                               subreddit_id : String,
                               thumbnail : String,
                               title : String,
                               url : String
                             )

}
