package edu.umd.cs.hcil.analytics.tokenizer.reddit

import java.io.StringReader

import scala.collection.JavaConverters._
import edu.umd.cs.hcil.models.reddit.RedditModel
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.nibor.autolink.{LinkExtractor, LinkType}

/**
  * Created by cbuntain on 7/31/18.
  */
object RedditTokenizer {

  @transient lazy val linkExtractor = LinkExtractor.builder()
    .linkTypes(Set(LinkType.URL, LinkType.WWW).asJava)
    .build()

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

  def tokenize(post: RedditModel) : Array[String] = {

    val text : String = post.text.getOrElse(null)

    val tokens = if ( text != null ) {

      val unescapedText = org.apache.commons.lang3.StringEscapeUtils.unescapeHtml4(text)
      val links = linkExtractor.extractLinks(unescapedText).iterator().asScala.toArray

      var tokensTmp = Array[String]()

      var lastIndex = 0
      for ( link <- links ) {
        val substr = unescapedText.substring(lastIndex, link.getBeginIndex)

        tokensTmp = tokensTmp ++ tokenizeString(substr) :+ unescapedText.substring(link.getBeginIndex, link.getEndIndex)

        lastIndex = link.getEndIndex
      }

      tokensTmp ++ tokenizeString(unescapedText.substring(lastIndex, unescapedText.length))
    } else {
      Array[String]()
    }

    return tokens

  }

}
