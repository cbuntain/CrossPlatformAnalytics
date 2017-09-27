import edu.umd.cs.hcil.models.fourchan.ThreadParser
import org.jsoup.Jsoup
import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import org.junit.Test
import org.junit.Before

/**
  * Created by cbuntain on 9/13/17.
  */
class FourChanParserTest extends AssertionsForJUnit {

  val fourchanText =
    """
      |{"posts":[{"h":497,"w":631,"no":79738047,"com":"I&#039;ll just leave this here.","ext":".jpg","md5":"lm+P2D3q8Oldsju20Er/EA==","now":"07/04/16(Mon)13:23:53","tim":1467653033949,"name":"Anonymous","time":1467653033,"tn_h":196,"tn_w":250,"fsize":33577,"resto":0,"closed":1,"images":0,"country":"US","replies":3,"archived":1,"filename":"13567471_517010865171620_2563271332446102973_n","bumplimit":0,"imagelimit":0,"archived_on":1467655773,"country_name":"United States","semantic_url":"ill-just-leave-this-here"},{"no":79738142,"com":"<a href=\"#p79738047\" class=\"quotelink\">&gt;&gt;79738047</a><br>go check how the pound fares towards the euro you burger","now":"07/04/16(Mon)13:25:00","name":"Anonymous","time":1467653100,"resto":79738047,"country":"GB","country_name":"United Kingdom"},{"no":79738222,"com":"<a href=\"#p79738142\" class=\"quotelink\">&gt;&gt;79738142</a><br>Asian countries devalue their currency - good thing.<br><br>Western European currency loses some value - oy vey it&#039;s the shoah all over again!","now":"07/04/16(Mon)13:25:56","name":"Anonymous","time":1467653156,"resto":79738047,"country":"US","country_name":"United States"},{"no":79739142,"com":"<a href=\"#p79738142\" class=\"quotelink\">&gt;&gt;79738142</a><br><span class=\"quote\">&gt;The pound meme</span><br><br>Still at it huh?","now":"07/04/16(Mon)13:35:43","name":"Anonymous","time":1467653743,"resto":79738047,"country":"US","country_name":"United States"}]}
    """.stripMargin

  @Test def verifyParser() : Unit = {

    val inputFileStrings = List[String](fourchanText)

    for ( threadJson <- inputFileStrings ) {
      val threadObject = ThreadParser.parseJson(threadJson)

      assertEquals(4, threadObject.posts.length)
      for ( post <- threadObject.posts ) {
        assertEquals("Anonymous", post.name)
      }
    }
  }

  @Test def verifyComText() : Unit = {

    val inputFileStrings = List[String](fourchanText)

    for ( threadJson <- inputFileStrings ) {
      val threadObject = ThreadParser.parseJson(threadJson)

      assertEquals(4, threadObject.posts.length)
      for ( post <- threadObject.posts ) {
        assertEquals("Anonymous", post.name)
      }

      val firstPost = threadObject.posts.head
      val headText = Jsoup.parse(firstPost.com).text()
      println(headText)
      assertEquals("I'll just leave this here.", headText)
      assertEquals(headText, ThreadParser.getPlainText(firstPost))

      val nextText = Jsoup.parse(threadObject.posts(1).com).text()
      println(nextText)
      assertEquals(">>79738047 go check how the pound fares towards the euro you burger", nextText)
    }
  }

}
