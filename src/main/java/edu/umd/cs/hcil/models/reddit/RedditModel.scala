package edu.umd.cs.hcil.models.reddit

/**
  * Created by cbuntain on 5/22/18.
  */
trait RedditModel {
  def id: String
  def created: Option[Long]
  def created_utc: Option[Long]
  def author: String
  def score: Long
  def subreddit: Option[String]
  def subreddit_id: Option[String]
  def text: Option[String]
}
