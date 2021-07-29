package org.thp.scalligraph.models

import org.apache.commons.text.similarity.LevenshteinDistance
import org.apache.tinkerpop.gremlin.process.traversal.P

import java.util.function.BiPredicate
import scala.annotation.tailrec

object TextPredicate {
  val containsPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = t.contains(u)
    override def negate(): BiPredicate[String, String] = notContainsPredicate
  }

  val notContainsPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = !t.contains(u)
    override def negate(): BiPredicate[String, String] = containsPredicate
  }

  val startsWithPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = t.startsWith(u)
    override def negate(): BiPredicate[String, String] = notStartsWithPredicate
  }

  val notStartsWithPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = !t.startsWith(u)
    override def negate(): BiPredicate[String, String] = startsWithPredicate
  }

  val endsWithPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = t.endsWith(u)
    override def negate(): BiPredicate[String, String] = notEndsWithPredicate
  }

  val notEndsWithPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = !t.endsWith(u)
    override def negate(): BiPredicate[String, String] = endsWithPredicate
  }

  val regexPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = t.matches(u)
    override def negate(): BiPredicate[String, String] = notRegexPredicate
  }

  val notRegexPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = !t.matches(u)
    override def negate(): BiPredicate[String, String] = regexPredicate
  }

  private def isNear(t: String, u: String): Boolean = {
    val distance =
      if (t.length < 3) 0
      else if (t.length < 6) 1
      else 2
    LevenshteinDistance.getDefaultInstance()(u, t) <= distance
  }
  val fuzzyPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = isNear(u, t)
    override def negate(): BiPredicate[String, String] = notFuzzyPredicate
  }
  val notFuzzyPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = isNear(u, t)
    override def negate(): BiPredicate[String, String] = fuzzyPredicate
  }

  def contains(value: String): P[String]      = new P[String](containsPredicate, value)
  def notContains(value: String): P[String]   = new P[String](notContainsPredicate, value)
  def startsWith(value: String): P[String]    = new P[String](startsWithPredicate, value)
  def notStartsWith(value: String): P[String] = new P[String](notStartsWithPredicate, value)
  def endsWith(value: String): P[String]      = new P[String](endsWithPredicate, value)
  def notEndsWith(value: String): P[String]   = new P[String](notEndsWithPredicate, value)
  def regex(value: String): P[String]         = new P[String](regexPredicate, value)
  def notRegex(value: String): P[String]      = new P[String](notRegexPredicate, value)
  def fuzzy(value: String): P[String]         = new P[String](fuzzyPredicate, value)
  def notFuzzy(value: String): P[String]      = new P[String](notFuzzyPredicate, value)
}

object FullTextPredicate {

  @tailrec
  private def tokenExists(text: String, test: String => Boolean): Boolean = {
    val trimmedText = text.dropWhile(!_.isLetterOrDigit)
    val token       = trimmedText.takeWhile(_.isLetterOrDigit)
    if (test(token)) true
    else {
      val remainingText = trimmedText.drop(token.length)
      if (remainingText.nonEmpty) tokenExists(remainingText, test)
      else false
    }
  }

  val containsPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean = {
      val lowerT = t.toLowerCase
      val lowerU = u.toLowerCase
      lowerT.contains(lowerU) && tokenExists(lowerT, _ == lowerU)
    }
    override def negate(): BiPredicate[String, String] = notContainsPredicate
  }

  val notContainsPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean = {
      val lowerT = t.toLowerCase
      val lowerU = u.toLowerCase
      !tokenExists(lowerT, _ == lowerU)
    }
    override def negate(): BiPredicate[String, String] = containsPredicate
  }

  val startsWithPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean = {
      val lowerT = t.toLowerCase
      val lowerU = u.toLowerCase
      lowerT.contains(lowerU) && tokenExists(lowerT, _.startsWith(lowerU))
    }
    override def negate(): BiPredicate[String, String] = notStartsWithPredicate
  }

  val notStartsWithPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean = {
      val lowerT = t.toLowerCase
      val lowerU = u.toLowerCase
      !tokenExists(lowerT, _.startsWith(lowerU))
    }
    override def negate(): BiPredicate[String, String] = startsWithPredicate
  }

  val endsWithPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean = {
      val lowerT = t.toLowerCase
      val lowerU = u.toLowerCase
      lowerT.contains(lowerU) && tokenExists(lowerT, _.endsWith(lowerU))
    }
    override def negate(): BiPredicate[String, String] = notEndsWithPredicate
  }

  val notEndsWithPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean = {
      val lowerT = t.toLowerCase
      val lowerU = u.toLowerCase
      !tokenExists(lowerT, _.endsWith(lowerU))
    }
    override def negate(): BiPredicate[String, String] = endsWithPredicate
  }

  val regexPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = tokenExists(t.toLowerCase, _.matches(u))
    override def negate(): BiPredicate[String, String] = notRegexPredicate
  }

  val notRegexPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean   = !tokenExists(t.toLowerCase, _.matches(u))
    override def negate(): BiPredicate[String, String] = regexPredicate
  }

  private def isNear(t: String, u: String): Boolean = {
    val distance =
      if (t.length < 3) 0
      else if (t.length < 6) 1
      else 2
    LevenshteinDistance.getDefaultInstance()(u, t) <= distance
  }
  val fuzzyPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean = {
      val lowerT = t.toLowerCase
      val lowerU = u.toLowerCase
      tokenExists(lowerT, isNear(lowerU, _))
    }
    override def negate(): BiPredicate[String, String] = notFuzzyPredicate
  }
  val notFuzzyPredicate: BiPredicate[String, String] = new BiPredicate[String, String] {
    override def test(t: String, u: String): Boolean = {
      val lowerT = t.toLowerCase
      val lowerU = u.toLowerCase
      !tokenExists(lowerT, isNear(lowerU, _))
    }
    override def negate(): BiPredicate[String, String] = fuzzyPredicate
  }

  def contains(value: String): P[String]      = new P[String](containsPredicate, value)
  def notContains(value: String): P[String]   = new P[String](notContainsPredicate, value)
  def startsWith(value: String): P[String]    = new P[String](startsWithPredicate, value)
  def notStartsWith(value: String): P[String] = new P[String](notStartsWithPredicate, value)
  def endsWith(value: String): P[String]      = new P[String](endsWithPredicate, value)
  def notEndsWith(value: String): P[String]   = new P[String](notEndsWithPredicate, value)
  def regex(value: String): P[String]         = new P[String](regexPredicate, value)
  def notRegex(value: String): P[String]      = new P[String](notRegexPredicate, value)
  def fuzzy(value: String): P[String]         = new P[String](fuzzyPredicate, value)
  def notFuzzy(value: String): P[String]      = new P[String](notFuzzyPredicate, value)
}
