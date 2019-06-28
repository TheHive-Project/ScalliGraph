package org.thp

import java.util.{Map => JMap}

import scala.util.{Failure, Success, Try}

import gremlin.scala.StepLabel

package object scalligraph {
  implicit class RichOptionTry[A](o: Option[Try[A]]) {
    def flip: Try[Option[A]] = o.fold[Try[Option[A]]](Success(None))(_.map(Some.apply))
  }
  implicit class RichOption[A](o: Option[A]) {
    def toTry(f: Failure[A]): Try[A] = o.fold[Try[A]](f)(Success.apply)
  }
//  implicit class RichSeqOrTry[A](s: Seq[Try[A]]) {
//    def flip: Try[Seq[A]] = s.foldLeft[Try[List[A]]](Success(Nil)) {
//      case (Success(l), e) ⇒ e.map(_ :: l)
//      case (f, _) ⇒ f
//    }
//  }
  implicit class RichSeq[A](s: TraversableOnce[A]) {

    def toTry[B](f: A => Try[B]): Try[Seq[B]] = s.foldLeft[Try[Seq[B]]](Success(Nil)) {
      case (Success(l), a) => f(a).map(l :+ _)
      case (failure, _)    => failure
    }
  }

  implicit class RichJMap(m: JMap[String, Any]) {
    def getValue[A](label: StepLabel[A]): A = m.get(label.name).asInstanceOf[A]
  }
}
