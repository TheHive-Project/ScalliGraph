package org.thp.scalligraph

import org.specs2.mutable.Specification
import org.thp.scalligraph.utils.Retry

import scala.util.Success

class RetryTest extends Specification {

  "Retry" should {
    "catch direct exception" in {
      var count = 0
      Retry(4)
        .on[ArithmeticException]
        .withTry {
          count += 1
          Success(12 / (count - 1))
        }
      count must_=== 2
    }

    "catch origin exception" in {
      var count = 0
      Retry(4)
        .on[ArithmeticException]
        .withTry {
          count += 1
          try Success(12 / (count - 1))
          catch { case t: Throwable => throw new RuntimeException("wrap", t) }
        }
      count must_=== 2
    }

    "retry until limit is reached" in {
      var count = 0
      Retry(4)
        .on[ArithmeticException]
        .withTry {
          count += 1
          Success(12 / (count - count))
        } must beFailedTry
      count must_=== 4
    }
  }

}
