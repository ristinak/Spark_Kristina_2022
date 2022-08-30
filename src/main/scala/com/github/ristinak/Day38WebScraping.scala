package com.github.ristinak

import org.jsoup.Jsoup

object Day38WebScraping extends App {
  println("Let's see about some webscraping!")

  val url = "https://en.wikipedia.org/wiki/Riga"

  //https://www.lihaoyi.com/post/ScrapingWebsitesusingScalaandJsoup.html

  //so html is just text but we want to parse it since it is structured text
  //so we parse it into some structure
  val doc = Jsoup.connect(url).get()
  //i could have used val html = Source.fromURL(url)  // but then I would still have to parse it

  println(doc.title())

  //usually we do not care about head when scraping
  //println(doc.head())

  //so you need to know what kind of element you want to find
  //here i search for ALL elements that have class attribute mw-body-content
  //I need to put . in front to indicate that is a class attribute
  //for id attribute i would use #my_id
  val content = doc.select(".mw-body-content")

  //sometimes elements do not have any attributes
  //then you would look for parent with some attributes and go down from there

  //so text will be in one big line
  //because html is generally without newlines
  //it fills boxes to the right and flows downright
  println(content.text())

  val anchors = doc.select("a") //find all anchor elements, notice no . and no # a - anchor
  //https://developer.mozilla.org/en-US/docs/Web/HTML/Element/a

    println(anchors.text()) //should print all urls in the main text
  //TODO extract anchors into array or some other sequence val arr = anchors.toArray(Element)

}
