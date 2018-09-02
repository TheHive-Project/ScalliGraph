package org.thp.scalligraph.models

import play.api.libs.json.Reads

import scala.collection.immutable

class Metrics private (values: immutable.Map[String, Long]) extends Map[String, Long] with immutable.MapLike[String, Long, Metrics] {
  override def empty: Metrics = Metrics.empty

  override def get(key: String): Option[Long] = values.get(key)

  override def iterator: Iterator[(String, Long)] = values.iterator

  override def +[V >: Long](kv: (String, V)): immutable.Map[String, V] =
    values + kv

  def +(kv: (String, Long))(implicit d: DummyImplicit): Metrics =
    new Metrics(values + kv)

  override def -(k: String) = new Metrics(values - k)
}

object Metrics {
  //  implicit val metricsFormat = {
  //    val metricsWrites = Writes[Metrics] { m ⇒
  //      JsObject(m.map {
  //        case (name, value) ⇒ name → JsNumber(value)
  //      })
  //    }
  implicit val metricsReads: Reads[Metrics] =
    Reads.mapReads[Long].map(new Metrics(_))
  //    Format(metricsReads, metricsWrites)
  //  }

  def apply(elems: (String, Long)*) = new Metrics(Map(elems: _*))

  def apply(map: Map[String, Long]) = new Metrics(map)

  def empty = new Metrics(Map.empty)
}
