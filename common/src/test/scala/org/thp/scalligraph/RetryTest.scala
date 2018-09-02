package org.thp.scalligraph

import play.api.test.PlaySpecification

class RetryTest extends PlaySpecification {

  "Retry" should {
    "catch direct exception" in {
      var count = 0
      Retry(4, classOf[ArithmeticException]) {
        count += 1
        12 / (count - 1)
      }
      count must_=== 2
    }

    "catch origin exception" in {
      var count = 0
      Retry(4, classOf[ArithmeticException]) {
        count += 1
        try { 12 / (count - 1) } catch { case t: Throwable ⇒ throw new RuntimeException("wrap", t) }
      }
      count must_=== 2
    }

    "retry until limit is reached" in {
      var count = 0
      Retry(4, classOf[ArithmeticException]) {
        count += 1
        12 / (count - count)
      } should throwA[ArithmeticException]
      count must_=== 5
    }
  }

}
