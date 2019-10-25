package org.thp.scalligraph.utils

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{blocking, Await, ExecutionContext, Future}
import scala.util.{Failure, Try}

import play.api.Logger

import akka.actor.{ActorSystem, Scheduler}
import akka.pattern.after

object Retry {

  def exceptionCheck(exceptions: Seq[Class[_]])(t: Throwable): Boolean =
    exceptions.isEmpty || exceptions.contains(t.getClass) || Option(t.getCause).exists(exceptionCheck(exceptions))

  val logger = Logger(getClass)

  def apply[T](maxTries: Int) = new Retry(maxTries, Nil)
}

class Retry(maxTries: Int, exceptions: Seq[Class[_]]) {
  import Retry._

  def apply[T](fn: => T): T                              = run(1, fn)
  def withTry[T](fn: => Try[T]): Try[T]                  = runTry(1, fn)
  def on[E <: Throwable](implicit manifest: Manifest[E]) = new Retry(maxTries, exceptions :+ manifest.runtimeClass)

  def delayed(delay: Int => FiniteDuration)(implicit system: ActorSystem) =
    new DelayRetry(maxTries, exceptions, system.scheduler, delay, system.dispatcher)

  def withBackoff(minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double)(implicit system: ActorSystem): DelayRetry =
    delayed { n => // from akka.pattern.BackoffSupervisor.calculateDelay
      val rnd                = 1.0 + ThreadLocalRandom.current().nextDouble() * randomFactor
      val calculatedDuration = Try(maxBackoff.min(minBackoff * math.pow(2, n.toDouble)) * rnd).getOrElse(maxBackoff)
      calculatedDuration match {
        case f: FiniteDuration => f
        case _                 => maxBackoff
      }
    }

  def delayed(delay: FiniteDuration)(implicit scheduler: Scheduler, ec: ExecutionContext) =
    new DelayRetry(maxTries, exceptions, scheduler, _ => delay, ec)

  private def run[T](currentTry: Int, f: => T): T =
    try f
    catch {
      case e: Throwable if currentTry < maxTries && exceptionCheck(exceptions)(e) =>
        logger.warn(s"An error occurs (${e.getMessage}), retrying ($currentTry)")
        run(currentTry + 1, f)
      case e: Throwable if currentTry < maxTries =>
        logger.error(s"uncaught error, not retrying", e)
        throw e
    }

  private def runTry[T](currentTry: Int, fn: => Try[T]): Try[T] =
    Try(fn).flatten.recoverWith {
      case e: Throwable if currentTry < maxTries && exceptionCheck(exceptions)(e) =>
        logger.warn(s"An error occurs (${e.getMessage}), retrying ($currentTry)")
        runTry(currentTry + 1, fn)
      case e: Throwable if currentTry < maxTries =>
        logger.error(s"uncaught error, not retrying", e)
        Failure(e)
    }
}

class DelayRetry(maxTries: Int, exceptions: Seq[Class[_]], scheduler: Scheduler, delay: Int => FiniteDuration, implicit val ec: ExecutionContext) {
  import Retry._

  def apply[T](fn: => Future[T]): Future[T] = run(1, fn)

  def sync[T](fn: => T): T =
    try fn
    catch {
      case e: Throwable if 1 < maxTries && exceptionCheck(exceptions)(e) =>
        logger.warn(s"An error occurs (${e.getMessage}), retrying (1)")
        blocking {
          Await.result(run(2, Future(fn)), Duration.Inf)
        }
      case e: Throwable if 1 < maxTries =>
        logger.error(s"uncaught error, not retrying", e)
        throw e
    }

  def withTry[T](fn: => Try[T]): Try[T] =
    Try(fn).flatten.recoverWith {
      case e: Throwable if 1 < maxTries && exceptionCheck(exceptions)(e) =>
        logger.warn(s"An error occurs (${e.getMessage}), retrying (1)")
        blocking {
          Try(Await.result(run(2, Future(fn.get)), Duration.Inf))
        }
      case e: Throwable if 1 < maxTries =>
        logger.error(s"uncaught error, not retrying", e)
        Failure(e)
    }

  def on[E <: Throwable](implicit manifest: Manifest[E]) = new DelayRetry(maxTries, exceptions :+ manifest.runtimeClass, scheduler, delay, ec)

  private def run[T](currentTry: Int, fn: => Future[T]): Future[T] =
    fn recoverWith {
      case e if currentTry < maxTries && exceptionCheck(exceptions)(e) =>
        logger.warn(s"An error occurs (${e.getMessage}), retrying ($currentTry)")
        after(delay(currentTry), scheduler)(run(currentTry + 1, fn))
      case e: Throwable if currentTry < maxTries =>
        logger.error(s"uncaught error, not retrying", e)
        Future.failed(e)
    }
}
