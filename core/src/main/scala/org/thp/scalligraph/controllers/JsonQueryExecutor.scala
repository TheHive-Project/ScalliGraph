package org.thp.scalligraph.controllers

import play.api.libs.json.{JsArray, JsNumber, JsValue}

import org.thp.scalligraph.query.QueryExecutor

abstract class JsonQueryExecutor extends QueryExecutor[JsValue] { thisExecutor ⇒
  override def toOutput(output: Any): JsValue = output match {
    case v: JsValue    ⇒ v
    case n: BigDecimal ⇒ JsNumber(n)
    case s: Seq[_]     ⇒ JsArray(s.map(toOutput))
    case o             ⇒ super.toOutput(o)
  }
}
