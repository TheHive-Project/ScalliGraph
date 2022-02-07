package org.thp

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

package object scalligraph {
  implicit class RichOptionTry[A](o: Option[Try[A]]) {
    def flip: Try[Option[A]] = o.fold[Try[Option[A]]](Success(None))(_.map(Some.apply))
  }
  implicit class RichTryOption[A](t: Try[Option[A]]) {
    def flip: Option[Try[A]] = t.fold(f => Some(Failure(f)), _.map(a => Success(a)))
  }

  implicit class RichOption[A](o: Option[A]) {
    def toTry(f: Failure[A]): Try[A] = o.fold[Try[A]](f)(Success.apply)
  }

  implicit class RichSeq[A](s: IterableOnce[A]) {
    def toTry[B](f: A => Try[B]): Try[Seq[B]] =
      s.iterator.foldLeft[Try[Seq[B]]](Success(Nil)) {
        case (Success(l), a) => f(a).map(l :+ _)
        case (failure, _)    => failure
      }
  }

  val timeUnitList: List[TimeUnit] = DAYS :: HOURS :: MINUTES :: SECONDS :: MILLISECONDS :: MICROSECONDS :: NANOSECONDS :: Nil
  implicit class RichFiniteDuration(duration: FiniteDuration) {
    def prettyPrint: String =
      timeUnitList
        .tails
        .collectFirst {
          case u +: r if duration >= FiniteDuration(1, u) =>
            val l = FiniteDuration(duration.toUnit(u).toLong, u)
            (l.toString, duration - l, r)
        }
        .fold(duration.toString) {
          case (s, r, ru +: _) if r > FiniteDuration(1, ru) => s"$s ${FiniteDuration(r.toUnit(ru).toLong, ru)}"
          case (s, _, _)                                    => s
        }
  }
}
