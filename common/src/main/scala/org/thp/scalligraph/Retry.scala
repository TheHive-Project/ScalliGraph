package org.thp.scalligraph

import play.api.Logger

object Retry {
  def exceptionCheck(exceptions: Seq[Class[_]])(t: Throwable): Boolean =
    exceptions.contains(t.getClass) || Option(t.getCause).exists(exceptionCheck(exceptions))

  val logger = Logger(getClass)

  def apply[T](n: Int, exceptions: Class[_]*)(fn: ⇒ T): T =
    try {
      fn
    } catch {
      case e: Throwable if n > 0 && exceptionCheck(exceptions)(e) ⇒
        logger.warn(s"An error occurs (${e.getMessage}), retrying ($n)")
        apply(n - 1, exceptions: _*)(fn)
      case e: Throwable if n > 0 ⇒
        logger.error(s"uncatch error, not retrying", e)
        throw e
    }
}
