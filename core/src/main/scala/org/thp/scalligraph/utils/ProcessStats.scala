package org.thp.scalligraph.utils

import scala.collection.concurrent.TrieMap
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class ProcessStats() {
  private class AVG(var count: Long = 0, var sum: Long = 0) {
    def +=(value: Long): Unit = {
      count += 1
      sum += value
    }
    def ++=(avg: AVG): Unit = {
      count += avg.count
      sum += avg.sum
    }
    def reset(): Unit = {
      count = 0
      sum = 0
    }
    def isEmpty: Boolean          = count == 0L
    override def toString: String = if (isEmpty) "0" else (sum / count).toString
  }

  private class StatEntry(
      var total: Long = -1,
      var nSuccess: Int = 0,
      var nFailure: Int = 0,
      global: AVG = new AVG,
      current: AVG = new AVG
  ) {
    def update(isSuccess: Boolean, time: Long): Unit = {
      if (isSuccess) nSuccess += 1
      else nFailure += 1
      current += time
    }

    def failure(): Unit = nFailure += 1

    def flush(): Unit = {
      global ++= current
      current.reset()
    }

    def isEmpty: Boolean = nSuccess == 0 && nFailure == 0

    def currentStats: String = {
      val totalTxt = if (total < 0) "" else s"/$total"
      val avg      = if (current.isEmpty) "" else s"(${current}ms)"
      s"${nSuccess + nFailure}$totalTxt$avg"
    }

    def setTotal(v: Long): Unit = total = v

    override def toString: String = {
      val totalTxt   = if (total < 0) s"/${nSuccess + nFailure}" else s"/$total"
      val avg        = if (global.isEmpty) "" else s" avg:${global}ms"
      val failureTxt = if (nFailure > 0) s"$nFailure failures" else ""
      s"$nSuccess$totalTxt$failureTxt$avg"
    }
  }

  private val stats: TrieMap[String, StatEntry] = TrieMap.empty
  private var stage: Option[String]             = None

  def `try`[A](name: String)(body: => Try[A]): Try[A] = {
    val start = System.currentTimeMillis()
    val ret   = body
    val time  = System.currentTimeMillis() - start
    stats.getOrElseUpdate(name, new StatEntry).update(ret.isSuccess, time)
    ret
  }

  def apply[A](name: String)(body: => A): A = {
    val start = System.currentTimeMillis()
    try {
      val ret  = body
      val time = System.currentTimeMillis() - start
      stats.getOrElseUpdate(name, new StatEntry).update(isSuccess = true, time)
      ret
    } catch {
      case error: Throwable =>
        val time = System.currentTimeMillis() - start
        stats.getOrElseUpdate(name, new StatEntry).update(isSuccess = false, time)
        throw error
    }
  }

  def failure(name: String): Unit = stats.getOrElseUpdate(name, new StatEntry).failure()

  def flush(): Unit = stats.foreach(_._2.flush())

  def setStage(s: String): Unit = stage = Some(s)
  def unsetStage(): Unit        = stage = None

  def showStats(): String =
    stats
      .collect {
        case (name, entry) if !entry.isEmpty => s"$name:${entry.currentStats}"
      }
      .mkString(stage.fold("")(s => s"[$s] "), " ", "")

  def showStats[A](interval: FiniteDuration, printMessage: String => Unit)(body: => A)(implicit ec: ExecutionContext): A = {
    var stop = false
    Future {
      blocking {
        while (!stop) {
          Thread.sleep(interval.toMillis)
          printMessage(showStats())
          flush()
        }
      }
    }
    try body
    finally stop = true
  }
  override def toString: String =
    stats
      .map {
        case (name, entry) => s"$name: $entry"
      }
      .toSeq
      .sorted
      .mkString(stage.fold("")(s => s"Stage: $s\n"), "\n", "")

  def setTotal(name: String, count: Long): Unit =
    stats.getOrElseUpdate(name, new StatEntry).setTotal(count)
}
