package org.thp.scalligraph.controllers

import org.thp.scalligraph.query.QueryExecutor
import play.api.libs.json.{JsArray, JsNumber, JsValue}

import scala.reflect.runtime.{universe ⇒ ru}

abstract class JsonQueryExecutor extends QueryExecutor[JsValue] { thisExecutor ⇒
  override def outputs: PartialFunction[ru.Type, Any ⇒ JsValue] = {
    case t if t <:< ru.typeOf[JsValue] ⇒
      value ⇒
        value.asInstanceOf[JsValue]
    case t if t weak_<:< ru.typeOf[BigDecimal] ⇒
      value ⇒
        JsNumber(value.asInstanceOf[BigDecimal])
    case t if t <:< ru.weakTypeOf[Seq[_]] ⇒
      val subOutput = getOutputFunction[Any](t.typeArgs.head)
      value ⇒
        JsArray(value.asInstanceOf[Seq[Any]].map(subOutput))
  }
}
