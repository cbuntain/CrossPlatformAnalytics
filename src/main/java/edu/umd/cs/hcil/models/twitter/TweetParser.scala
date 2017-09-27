package edu.umd.cs.hcil.models.twitter

import twitter4j.{Status, TwitterObjectFactory}

/**
  * Created by cbuntain on 7/21/17.
  */
object TweetParser {

  def parseJson(line : String) : Status = {
    try {
      val status = TwitterObjectFactory.createStatus(line)
      return status
    } catch {
      case npe : NullPointerException => return null
      case e : Exception => return null
    }
  }

  def getHashtagCount(status : Status) : Int = {
    val count = if ( status.getHashtagEntities != null ) {
      status.getHashtagEntities.size
    }  else {
      0
    }

    return count
  }

  def getUrlCount(status : Status) : Int = {
    val count = if ( status.getURLEntities != null ) {
      status.getURLEntities.size
    }  else {
      0
    }

    return count
  }

}
