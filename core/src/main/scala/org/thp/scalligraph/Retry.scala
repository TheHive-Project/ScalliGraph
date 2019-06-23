package org.thp.scalligraph

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Try}

import play.api.Logger

import akka.pattern.after
import akka.actor.Scheduler

object Retry {

  def exceptionCheck(exceptions: Seq[Class[_]])(t: Throwable): Boolean =
    exceptions.isEmpty || exceptions.contains(t.getClass) || Option(t.getCause).exists(exceptionCheck(exceptions))

  val logger = Logger(getClass)

  def apply[T](maxTries: Int) = new Retry(maxTries, Nil)
}

class Retry(maxTries: Int, exceptions: Seq[Class[_]]) {
  import Retry._

  def apply[T](fn: ⇒ T): T                               = run(1, fn)
  def withTry[T](fn: ⇒ Try[T]): Try[T]                   = runTry(1, fn)
  def on[E <: Throwable](implicit manifest: Manifest[E]) = new Retry(maxTries, exceptions :+ manifest.runtimeClass)

  def delayed(delay: Int ⇒ FiniteDuration)(implicit scheduler: Scheduler, ec: ExecutionContext) =
    new DelayRetry(maxTries, exceptions, scheduler, delay, ec)

  def delayed(delay: FiniteDuration)(implicit scheduler: Scheduler, ec: ExecutionContext) =
    new DelayRetry(maxTries, exceptions, scheduler, _ ⇒ delay, ec)

  private def run[T](currentTry: Int, f: ⇒ T): T =
    try f
    catch {
      case e: Throwable if currentTry < maxTries && exceptionCheck(exceptions)(e) ⇒
        logger.warn(s"An error occurs (${e.getMessage}), retrying ($currentTry)")
        run(currentTry + 1, f)
      case e: Throwable if currentTry < maxTries ⇒
        logger.error(s"uncaught error, not retrying", e)
        throw e
    }

  private def runTry[T](currentTry: Int, fn: ⇒ Try[T]): Try[T] =
    Try(fn).flatten.recoverWith {
      case e: Throwable if currentTry < maxTries && exceptionCheck(exceptions)(e) ⇒
        logger.warn(s"An error occurs (${e.getMessage}), retrying ($currentTry)")
        runTry(currentTry + 1, fn)
      case e: Throwable if currentTry < maxTries ⇒
        logger.error(s"uncaught error, not retrying", e)
        Failure(e)
    }
}

class DelayRetry(maxTries: Int, exceptions: Seq[Class[_]], scheduler: Scheduler, delay: Int ⇒ FiniteDuration, implicit val ec: ExecutionContext) {
  import Retry._

  def apply[T](fn: ⇒ Future[T]): Future[T]               = run(1, fn)
  def on[E <: Throwable](implicit manifest: Manifest[E]) = new DelayRetry(maxTries, exceptions :+ manifest.runtimeClass, scheduler, delay, ec)

  private def run[T](currentTry: Int, fn: ⇒ Future[T]): Future[T] =
    fn recoverWith {
      case e if currentTry < maxTries && exceptionCheck(exceptions)(e) ⇒
        logger.warn(s"An error occurs (${e.getMessage}), retrying ($currentTry)")
        after(delay(currentTry), scheduler)(run(currentTry + 1, fn))
      case e: Throwable if currentTry < maxTries ⇒
        logger.error(s"uncaught error, not retrying", e)
        Future.failed(e)
    }
}
