package org.thp.scalligraph.query

import java.lang.{Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.util.{Collection => JCollection, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}

import play.api.libs.json.{JsNumber, JsObject, Json, Writes}

import gremlin.scala.{__, By, GremlinScala, StepLabel, Vertex}
import org.scalactic.Accumulation.withGood
import org.scalactic.{Bad, One}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FObject, FString, Field, FieldsParser}
import org.thp.scalligraph.models.{ScalarSteps, ScalliSteps}
import org.thp.scalligraph.{BadRequestError, InvalidFormatAttributeError, Output}
import shapeless.HNil

object GroupAggregation {

  object AggObj {

    def unapply(field: Field): Option[(String, FObject)] = field match {
      case f: FObject =>
        f.get("_agg") match {
          case FString(name) => Some(name -> (f - "_agg"))
          case _             => None
        }
      case _ => None
    }
  }

  def groupAggregationParser: PartialFunction[String, FieldsParser[GroupAggregation[_, _]]] = {
    case "field" =>
      FieldsParser("FieldAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_aggName")(field),
            FieldsParser.string.on("_field")(field),
            aggregationFieldsParser.sequence.on("_select")(field)
          )((aggName, fieldName, subAgg) => FieldAggregation(aggName, fieldName, subAgg))
      }
    case "count" =>
      FieldsParser("CountAggregation") {
        case (_, field) => FieldsParser.string.optional.on("_aggName")(field).map(aggName => AggCount(aggName))
      }

  }

  def functionAggregationParser: PartialFunction[String, FieldsParser[Aggregation[_, _]]] = {
    case "avg" =>
      FieldsParser("AvgAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_aggName")(field),
            FieldsParser.string.on("_field")(field)
          )((aggName, fieldName) => AggAvg(aggName, fieldName))
      }
    case "min" =>
      FieldsParser("MinAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_aggName")(field),
            FieldsParser.string.on("_field")(field)
          )((aggName, fieldName) => AggMin(aggName, fieldName))
      }
    case "count" =>
      FieldsParser("CountAggregation") {
        case (_, field) => FieldsParser.string.optional.on("_aggName")(field).map(aggName => AggCount(aggName))
      }
  }

  val aggregationFieldsParser: FieldsParser[Aggregation[_, _]] = {
    def fp(name: String) =
      functionAggregationParser
        .orElse(groupAggregationParser)
        .applyOrElse(
          name,
          (name: String) =>
            FieldsParser[Aggregation[_, _]]("aggregation") {
              case _ => Bad(One(InvalidFormatAttributeError("_agg", "aggregation name", Set("avg", "min", "max", "count", "top"), FString(name))))
            }
        )
    FieldsParser("aggregation") {
      case (_, AggObj(name, field)) => fp(name)(field)
      //    case other => Bad(InvalidFormatAttributeError()) // TODO
    }
  }

  implicit val fieldsParser: FieldsParser[GroupAggregation[_, _]] = FieldsParser("aggregation") {
    case (_, AggObj(name, field)) => groupAggregationParser(name)(field)
    //    orElse Bad(InvalidFormatAttributeError()) // TODO
  }
}

abstract class GroupAggregation[T, R](name: String) extends Aggregation[T, R](name) {

  def get(properties: List[PublicProperty[_, _]], stepType: ru.Type, fromStep: ScalliSteps[_, _, _], authContext: AuthContext): Output[R] =
    output(apply(properties, stepType, fromStep, authContext).head())
}

abstract class Aggregation[T, R](val name: String) {

  def getProperty(properties: Seq[PublicProperty[_, _]], stepType: ru.Type, fieldName: String): PublicProperty[_, _] =
    properties
      .find(p => p.stepType =:= stepType && p.propertyName == fieldName)
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $stepType not found"))
  def apply(publicProperties: List[PublicProperty[_, _]], stepType: ru.Type, fromStep: ScalliSteps[_, _, _], authContext: AuthContext): ScalarSteps[T]
  def output(t: T): Output[R]
}

abstract class AggFunction[T, R](name: String) extends Aggregation[T, R](name) {
  implicit val numberWrites: Writes[Number] = Writes[Number] {
    case i: Integer => JsNumber(i.toInt)
    case l: JLong   => JsNumber(l.toLong)
    case f: JFloat  => JsNumber(f.toDouble)
    case o: Number  => JsNumber(o.doubleValue())
  }
}

case class AggSum(fieldName: String) extends AggFunction[Number, Number](s"sum_$fieldName") {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: ScalliSteps[_, _, _],
      authContext: AuthContext
  ): ScalarSteps[Number] = {
    val publicProperty = getProperty(publicProperties, stepType, fieldName)
    val raw            = fromStep.raw.asInstanceOf[GremlinScala[Vertex]]

    val result = publicProperty.mapping.graphTypeClass match {
      case c if c == classOf[Int]    => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Int]].sum[Number]
      case c if c == classOf[Long]   => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Long]].sum[Number]
      case c if c == classOf[Float]  => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Float]].sum[Number]
      case c if c == classOf[Double] => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Double]].sum[Number]
    }
    ScalarSteps(result)
  }
  override def output(t: Number): Output[Number] = Output(t) // TODO add aggregation name

}

case class AggAvg(aggName: Option[String], fieldName: String) extends AggFunction[JDouble, Double](aggName.getOrElse(s"avg_$fieldName")) {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: ScalliSteps[_, _, _],
      authContext: AuthContext
  ): ScalarSteps[JDouble] = {
    val publicProperty = getProperty(publicProperties, stepType, fieldName)
    val raw            = fromStep.raw.asInstanceOf[GremlinScala[Vertex]]

    val result = publicProperty.mapping.graphTypeClass match {
      case c if c == classOf[Int]    => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Int]].mean
      case c if c == classOf[Long]   => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Long]].mean
      case c if c == classOf[Float]  => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Float]].mean
      case c if c == classOf[Double] => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Double]].mean
    }
    ScalarSteps(result)
  }

  override def output(t: JDouble): Output[Double] = Output(t.toDouble) // TODO add aggregation name
}

case class AggMin(aggName: Option[String], fieldName: String) extends AggFunction[AnyVal, AnyVal](aggName.getOrElse(s"min_$fieldName")) {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: ScalliSteps[_, _, _],
      authContext: AuthContext
  ): ScalarSteps[AnyVal] = {
    val publicProperty = getProperty(publicProperties, stepType, fieldName)
    val raw            = fromStep.raw.asInstanceOf[GremlinScala[Vertex]]

    val result = publicProperty.mapping.graphTypeClass match {
      case c if c == classOf[Int]    => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Int]].min[JInt]
      case c if c == classOf[Long]   => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Long]].min[JLong]
      case c if c == classOf[Float]  => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Float]].min[JFloat]
      case c if c == classOf[Double] => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Double]].min[JDouble]
      case _                         => ???
    }
    ScalarSteps(result.asInstanceOf[GremlinScala[AnyVal]])
  }
  override def output(t: AnyVal): Output[AnyVal] = Output(t)(Output.valWrites.transform(v => Json.obj(name -> v)))
}

case class AggMax(fieldName: String) extends AggFunction[AnyVal, AnyVal](s"max_$fieldName") {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: ScalliSteps[_, _, _],
      authContext: AuthContext
  ): ScalarSteps[AnyVal] = {
    val publicProperty = getProperty(publicProperties, stepType, fieldName)
    val raw            = fromStep.raw.asInstanceOf[GremlinScala[Vertex]]

    val result = publicProperty.mapping.graphTypeClass match {
      case c if c == classOf[Int]    => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Int]].max[JInt]
      case c if c == classOf[Long]   => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Long]].max[JLong]
      case c if c == classOf[Float]  => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Float]].max[JFloat]
      case c if c == classOf[Double] => publicProperty.get(raw, authContext).asInstanceOf[GremlinScala[Double]].max[JDouble]
      case _                         => ???
    }
    ScalarSteps(result.asInstanceOf[GremlinScala[AnyVal]])
  }
  override def output(t: AnyVal): Output[AnyVal] = Output(t)(Output.valWrites.transform(v => Json.obj(name -> v)))
}

case class AggCount(aggName: Option[String]) extends GroupAggregation[JLong, Long](aggName.getOrElse("count")) {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: ScalliSteps[_, _, _],
      authContext: AuthContext
  ): ScalarSteps[JLong] =
    ScalarSteps(fromStep.raw.count())
  override def output(t: JLong): Output[Long] = Output(t.toLong, Json.obj(name -> t.toLong))

}
//case class AggTop[T](fieldName: String) extends AggFunction[T](s"top_$fieldName")

case class FieldAggregation(aggName: Option[String], fieldName: String, subAggs: Seq[Aggregation[_, _]])
    extends GroupAggregation[JList[JCollection[Any]], Map[Any, Map[String, Any]]](aggName.getOrElse(s"field_$fieldName")) {

  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: ScalliSteps[_, _, _],
      authContext: AuthContext
  ): ScalarSteps[JList[JCollection[Any]]] = {
    val property     = getProperty(publicProperties, stepType, fieldName).asInstanceOf[PublicProperty[_, Any]]
    val elementLabel = StepLabel[Vertex]()
    val groupedVertices: GremlinScala[JMap.Entry[Any, JCollection[Any]]] = property
      .get(fromStep.raw.asInstanceOf[GremlinScala.Aux[Vertex, HNil]].as(elementLabel), authContext)
      .group(By(), By(__.select(elementLabel).fold()))
      .unfold[JMap.Entry[Any, JCollection[Any]]] // Map.Entry[K, List[V]]

    ScalarSteps(groupedVertices)
      .project[Any](
        By(__[JMap.Entry[Any, Any]].selectKeys) +: subAggs
          .map(a => By(a.apply(publicProperties, stepType, ScalarSteps(__[JMap[Any, Any]].selectValues.unfold()), authContext).raw)): _*
      )
      .fold
  }

  override def output(l: JList[JCollection[Any]]): Output[Map[Any, Map[String, Any]]] = {
    val subMap: Map[Any, Output[Map[String, Any]]] = l
      .asScala
      .map(_.asScala)
      .flatMap { e =>
        val key = e.head
        if (key != "") { // maybe should be compared with property.mapping.noValue but property is not accessible here
          val values = subAggs
            .asInstanceOf[Seq[Aggregation[Any, Any]]]
            .zip(e.tail)
            .map { case (a, r) => a.name -> a.output(r).toOutput }
            .toMap
          val jsValues =
            subAggs
              .asInstanceOf[Seq[Aggregation[Any, Any]]]
              .zip(e.tail)
              .foldLeft(JsObject.empty) {
                case (acc, (ar, r)) =>
                  ar.output(r).toJson match {
                    case o: JsObject => acc ++ o
                    case v           => acc + (ar.name -> v)
                  }
              }
          Some(key -> Output(values, jsValues))
        } else None
      }
      .toMap

    val native: Map[Any, Map[String, Any]] = subMap.map { case (k, v) => k -> v.toOutput }
    val json: JsObject                     = JsObject(subMap.map { case (k, v) => k.toString -> v.toJson }.toMap)
    //Json.obj(name -> JsObject(subMap.map { case (k, v) ⇒ k.toString -> v.toJson }))
    Output(native, json)
  }
}

case class CategoryAggregation() // Map[String,
case class TimeAggregation()     // Map[Date,
