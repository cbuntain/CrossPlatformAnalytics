package edu.umd.cs.hcil.analytics.tokenizer.twitter

import java.io.StringReader

import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import twitter4j.Status

/**
  * Created by cbuntain on 7/23/18.
  */
object TwitterTokenizer {

  def tokenizeString(text : String) : Array[String] = {

    val tokenizer = new ICUTokenizer()

    tokenizer.setReader(new StringReader(text))
    tokenizer.reset()

    val attr = tokenizer.addAttribute(classOf[CharTermAttribute])

    var tokens = Array[String]()
    while(tokenizer.incrementToken()) {
      // Grab the term
      val term = attr.toString()

      tokens = tokens :+ term
    }

    return tokens
  }

  def tokenize(tweet: Status) : Array[String] = {

    val text = tweet.getText

    val entities : Array[(Int,String,String)] = (
        tweet.getHashtagEntities.map(e => (e.getStart, "#" + e.getText, "#" + e.getText)) ++
        tweet.getMediaEntities.map(e => (e.getStart, e.getText, e.getText)) ++
        tweet.getURLEntities.map(e => (e.getStart, e.getURL, e.getExpandedURL)) ++
        tweet.getUserMentionEntities.map(e => (e.getStart, "@" + e.getText, "@" + e.getText)) ++
        tweet.getSymbolEntities.map(e => (e.getStart, e.getText, e.getText))
      ).distinct.sortBy(entity => entity._1)

    var tokens = Array[String]()

    var lastIndex = 0
    for ( e <- entities ) {
      if ( text.contains(e._2 ) ) {
        val start = text.indexOf(e._2, lastIndex)

        if ( start >= 0 ) {
          val sub = text.substring(lastIndex, start)

          if (sub.length > 0) {
            tokens = tokens ++ tokenizeString(sub)
          }
          tokens = tokens :+ e._3

          lastIndex = start + e._2.length
        }
      }
    }

    tokens = tokens ++ tokenizeString(text.substring(lastIndex, text.length))

    return tokens
  }

  def main(argv : Array[String]) : Unit = {

    val textPath = argv(1)
    val tweets = scala.io.Source.fromFile(textPath).getLines

    for ( s <- tweets ) {

      val status = TweetParser.parseJson(s)
      println(status.getText)
      for ( token <- tokenize(status) ) {
        println("\t" + token)
      }
    }
  }
}
