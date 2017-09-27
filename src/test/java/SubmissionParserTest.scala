import edu.umd.cs.hcil.models.reddit.SubmissionParser
import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import org.junit.Test


/**
  * Created by cbuntain on 9/13/17.
  */
class SubmissionParserTest extends AssertionsForJUnit {

  val submissionJson =
    """
      {"author":"Nuen","subreddit":"k_on","hidden":false,"edited":false,"is_self":false,"id":"5wwvox","link_flair_css_class":"art","score":37,"selftext":"","contest_mode":false,"brand_safe":false,"domain":"i.redd.it","retrieved_on":1492364238,"archived":false,"created_utc":1488385003,"secure_media_embed":{},"media":null,"locked":false,"distinguished":null,"secure_media":null,"link_flair_text":"Artwork","over_18":false,"url":"https://i.redd.it/r0h5pdpgmtiy.jpg","thumbnail":"https://b.thumbs.redditmedia.com/pRC8ksZLWln2u9Ulu7rO6pgGKz_zRRUdhv6HvdlGHzg.jpg","author_flair_text":"","quarantine":false,"suggested_sort":null,"spoiler":false,"subreddit_id":"t5_2tpfm","preview":{"enabled":true,"images":[{"variants":{},"id":"jMfSHQ5YjYeLm9gyTPc0DYJEeG7YZIzPYn0OyAJ3Fyg","resolutions":[{"width":108,"url":"https://i.redditmedia.com/nq3APD9jO6ZzVpbgS6khN3OWzuZtHxEFlvx1hZP1MaQ.jpg?fit=crop&amp;crop=faces%2Centropy&amp;arh=2&amp;w=108&amp;s=f0c5492ba194fdb3e471ab08bae39c99","height":144},{"width":216,"height":289,"url":"https://i.redditmedia.com/nq3APD9jO6ZzVpbgS6khN3OWzuZtHxEFlvx1hZP1MaQ.jpg?fit=crop&amp;crop=faces%2Centropy&amp;arh=2&amp;w=216&amp;s=ceac62320eefd7750db2c6e87436e7c2"},{"width":320,"url":"https://i.redditmedia.com/nq3APD9jO6ZzVpbgS6khN3OWzuZtHxEFlvx1hZP1MaQ.jpg?fit=crop&amp;crop=faces%2Centropy&amp;arh=2&amp;w=320&amp;s=7bafe732d92f1972d473f530892ff721","height":428},{"height":856,"url":"https://i.redditmedia.com/nq3APD9jO6ZzVpbgS6khN3OWzuZtHxEFlvx1hZP1MaQ.jpg?fit=crop&amp;crop=faces%2Centropy&amp;arh=2&amp;w=640&amp;s=c78065f46a1f3aba241f866fb74b33ba","width":640},{"height":1285,"url":"https://i.redditmedia.com/nq3APD9jO6ZzVpbgS6khN3OWzuZtHxEFlvx1hZP1MaQ.jpg?fit=crop&amp;crop=faces%2Centropy&amp;arh=2&amp;w=960&amp;s=a1113fa772e5a4ed1bd63b0eb44ae415","width":960},{"url":"https://i.redditmedia.com/nq3APD9jO6ZzVpbgS6khN3OWzuZtHxEFlvx1hZP1MaQ.jpg?fit=crop&amp;crop=faces%2Centropy&amp;arh=2&amp;w=1080&amp;s=b90325425a1527fe4ef27b2598c3f48a","height":1445,"width":1080}],"source":{"width":1936,"height":2592,"url":"https://i.redditmedia.com/nq3APD9jO6ZzVpbgS6khN3OWzuZtHxEFlvx1hZP1MaQ.jpg?s=ad5da161ac13096d962fe9ce04df3483"}}]},"stickied":false,"media_embed":{},"title":"Daily ED Outfit #457: 10/10 would sail with","permalink":"/r/k_on/comments/5wwvox/daily_ed_outfit_457_1010_would_sail_with/","author_flair_css_class":"singingyui","hide_score":false,"post_hint":"image","num_comments":3,"gilded":0}
    """.stripMargin

  @Test def verifyParser() : Unit = {

    val inputFileStrings = List[String](submissionJson)

    for ( threadJson <- inputFileStrings ) {
      val threadObject = SubmissionParser.parseJson(threadJson)

      assertEquals("Daily ED Outfit #457: 10/10 would sail with", threadObject.title)
    }
  }


}
