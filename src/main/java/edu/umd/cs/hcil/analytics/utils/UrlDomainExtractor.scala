package edu.umd.cs.hcil.analytics.utils

import org.apache.commons.validator.routines.UrlValidator

/**
  * Created by cbuntain on 4/26/18.
  */
object UrlDomainExtractor {

  val secondLevelTlds = List("ar", "at", "au", "br", "de", "es", "fr", "il", "jp", "kr", "nz", "ru", "tr", "ua", "uk", "us", "za")

  val urlValidator = new UrlValidator(Array[String]("http", "https"))

  def getTLD(url: String) : Option[String] = {
    val colonIndex = url.indexOf(":")
    val strippedUrl = if ( colonIndex < 0 ) {
      url
    } else {
      url.substring(colonIndex+3)
    }

    val slashIndex = strippedUrl.indexOf("/")
    val domain = if ( slashIndex < 0 ) {
      strippedUrl
    } else {
      strippedUrl.substring(0, slashIndex)
    }

    val domainParts = domain.split("\\.")
    val partCount = domainParts.length

    if ( partCount > 2 ) {
      if ( secondLevelTlds.contains(domainParts(partCount-1)) ) {
        val candidate = "%s.%s.%s".format(domainParts(partCount-3), domainParts(partCount-2), domainParts(partCount-1))

        if (urlValidator.isValid("http://" + candidate)) {
          Some(candidate)
        } else {
          None
        }
      } else {
        val candidate = "%s.%s".format(domainParts(partCount-2), domainParts(partCount-1))

        if (urlValidator.isValid("http://" + candidate)) {
          Some(candidate)
        } else {
          None
        }
      }
    } else if (urlValidator.isValid("http://" + domain)) {
      Some(domain)
    } else {
      None
    }
  }

}
