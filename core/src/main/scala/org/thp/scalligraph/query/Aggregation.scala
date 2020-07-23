package org.thp.scalligraph.query

import java.lang.{Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.time.temporal.ChronoUnit
import java.util.{Calendar, Date, Collection => JCollection, List => JList, Map => JMap}

import gremlin.scala.{__, By, StepLabel, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.{Order, Scope}
import org.scalactic.Accumulation.withGood
import org.scalactic.{Good, One, Or}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.{Database, UniMapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{BaseVertexSteps, Traversal}
import org.thp.scalligraph.{BadRequestError, InvalidFormatAttributeError}
import play.api.Logger
import play.api.libs.json.{JsNumber, JsObject, Json, Writes}

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}
import scala.util.Try
import scala.util.matching.Regex

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

  val intervalParser: FieldsParser[(Long, ChronoUnit)] = FieldsParser[(Long, ChronoUnit)]("interval") {
    case (_, f) =>
      withGood(
        FieldsParser.long.optional.on("_interval")(f),
        FieldsParser[ChronoUnit]("chronoUnit") {
          case (_, f @ FString(value)) =>
            Or.from(
              Try(ChronoUnit.valueOf(value)).toOption,
              One(InvalidFormatAttributeError("_unit", "chronoUnit", ChronoUnit.values.toSet.map((_: ChronoUnit).toString), f))
            )
        }.on("_unit")(f)
      )((i, u) => i.getOrElse(0L) -> u)
  }

  val intervalRegex: Regex = "(\\d+)([smhdwMy])".r

  val mergedIntervalParser: FieldsParser[(Long, ChronoUnit)] = FieldsParser[(Long, ChronoUnit)]("interval") {
    case (_, FString(intervalRegex(interval, unit))) =>
      Good(unit match {
        case "s" => interval.toLong -> ChronoUnit.SECONDS
        case "m" => interval.toLong -> ChronoUnit.MINUTES
        case "h" => interval.toLong -> ChronoUnit.HOURS
        case "d" => interval.toLong -> ChronoUnit.DAYS
        case "w" => interval.toLong -> ChronoUnit.WEEKS
        case "M" => interval.toLong -> ChronoUnit.MONTHS
        case "y" => interval.toLong -> ChronoUnit.YEARS
      })
  }

  def fieldsParser(filterParser: FieldsParser[InputFilter]): FieldsParser[GroupAggregation[_, _, _]] = FieldsParser("aggregation") {
    case (_, AggObj("field", field)) =>
      FieldsParser("FieldAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            FieldsParser.string.on("_field")(field),
            FieldsParser.string.sequence.on("_order")(field).orElse(FieldsParser.string.on("_order").map("order")(Seq(_))(field)),
            FieldsParser.long.optional.on("_size")(field),
            fieldsParser(filterParser).sequence.on("_select")(field)
          )((aggName, fieldName, order, size, subAgg) => FieldAggregation(aggName, fieldName, order, size, subAgg))
      }(field)
    case (_, AggObj("count", field)) =>
      FieldsParser("CountAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            filterParser.optional.on("_query")(field)
          )((aggName, filter) => AggCount(aggName, filter))
      }(field)
    case (_, AggObj("avg", field)) =>
      FieldsParser("AvgAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            FieldsParser.string.on("_field")(field),
            filterParser.optional.on("_query")(field)
          )((aggName, fieldName, filter) => AggAvg(aggName, fieldName, filter))
      }(field)
    case (_, AggObj("min", field)) =>
      FieldsParser("MinAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            FieldsParser.string.on("_field")(field),
            filterParser.optional.on("_query")(field)
          )((aggName, fieldName, filter) => AggMin(aggName, fieldName, filter))
      }(field)
    case (_, AggObj("max", field)) =>
      FieldsParser("MaxAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            FieldsParser.string.on("_field")(field),
            filterParser.optional.on("_query")(field)
          )((aggName, fieldName, filter) => AggMax(aggName, fieldName, filter))
      }(field)
    case (_, AggObj("time", field)) =>
      FieldsParser("TimeAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            FieldsParser.string.sequence.on("_fields")(field),
            mergedIntervalParser.on("_interval").orElse(intervalParser)(field),
            fieldsParser(filterParser).sequence.on("_select")(field)
          )((aggName, fieldNames, intervalUnit, subAgg) => TimeAggregation(aggName, fieldNames.head, intervalUnit._1, intervalUnit._2, subAgg))
      }(field)
  }
}

abstract class GroupAggregation[TD, TG, R](name: String) extends Aggregation[TD, TG, R](name) {

  def get(
      db: Database,
      properties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: BaseVertexSteps,
      authContext: AuthContext
  ): Output[R] =
    output(apply(db, properties, stepType, fromStep, authContext).head())
}

abstract class Aggregation[TD, TG, R](val name: String) {

  def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: BaseVertexSteps,
      authContext: AuthContext
  ): Traversal[TD, TG]

  def output(t: TD): Output[R]
}

abstract class AggFunction[TD, TG, R](name: String) extends GroupAggregation[TD, TG, R](name) {
  implicit val numberWrites: Writes[Number] = Writes[Number] {
    case i: JInt    => JsNumber(i.toInt)
    case l: JLong   => JsNumber(l.toLong)
    case f: JFloat  => JsNumber(f.toDouble)
    case d: JDouble => JsNumber(d.doubleValue())
    case o: Number  => JsNumber(o.doubleValue())
  }
}

case class AggSum(fieldName: String, filter: Option[InputFilter]) extends AggFunction[Number, Number, Number](s"sum_$fieldName") {
  override def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: BaseVertexSteps,
      authContext: AuthContext
  ): Traversal[Number, Number] = {
    val property = PublicProperty.getPropertyTraversal(
      publicProperties,
      stepType,
      filter.fold(fromStep)(f => f.apply(db, publicProperties, stepType, fromStep, authContext)),
      fieldName,
      authContext
    )

    property
      .cast(UniMapping.int)
      .map(_.sum[Number]())
      .orElse(property.cast(UniMapping.long).map(_.sum[Number]()))
      .orElse(property.cast(UniMapping.float).map(_.sum[Number]()))
      .orElse(property.cast(UniMapping.double).map(_.sum[Number]()))
      .getOrElse(throw BadRequestError(s"Property $fieldName in $fromStep can't be cast to number. Sum aggregation is not applicable"))
  }
  override def output(t: Number): Output[Number] = Output(t) // TODO add aggregation name

}

case class AggAvg(aggName: Option[String], fieldName: String, filter: Option[InputFilter])
    extends AggFunction[Double, JDouble, Double](aggName.getOrElse(s"avg_$fieldName")) {
  override def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: BaseVertexSteps,
      authContext: AuthContext
  ): Traversal[Double, JDouble] = {
    val property = PublicProperty.getPropertyTraversal(
      publicProperties,
      stepType,
      filter.fold(fromStep)(f => f.apply(db, publicProperties, stepType, fromStep, authContext)),
      fieldName,
      authContext
    )

    property
      .cast(UniMapping.int)
      .map(_.mean)
      .orElse(property.cast(UniMapping.long).map(_.mean))
      .orElse(property.cast(UniMapping.float).map(_.mean))
      .orElse(property.cast(UniMapping.double).map(_.mean))
      .getOrElse(throw BadRequestError(s"Property $fieldName in $fromStep can't be cast to number. Avg aggregation is not applicable"))
  }

  override def output(t: Double): Output[Double] = Output(t) // TODO add aggregation name
}

case class AggMin(aggName: Option[String], fieldName: String, filter: Option[InputFilter])
    extends AggFunction[Number, Number, Number](aggName.getOrElse(s"min_$fieldName")) {
  override def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: BaseVertexSteps,
      authContext: AuthContext
  ): Traversal[Number, Number] = {
    val property = PublicProperty.getPropertyTraversal(
      publicProperties,
      stepType,
      filter.fold(fromStep)(f => f.apply(db, publicProperties, stepType, fromStep, authContext)),
      fieldName,
      authContext
    )

    property
      .cast(UniMapping.int)
      .flatMap(_.min[JInt]().cast(UniMapping.jint))
      .orElse(property.cast(UniMapping.long).flatMap(_.min[JLong]().cast(UniMapping.jlong)))
      .orElse(property.cast(UniMapping.float).flatMap(_.min[JFloat]().cast(UniMapping.jfloat)))
      .orElse(property.cast(UniMapping.double).flatMap(_.min[JDouble]().cast(UniMapping.jdouble)))
      .getOrElse(throw BadRequestError(s"Property $fieldName in $fromStep can't be cast to number. Min aggregation is not applicable"))
      .asInstanceOf[Traversal[Number, Number]]
  }
  override def output(t: Number): Output[Number] = Output(t)(Writes(v => Json.obj(name -> v)))
}

case class AggMax(aggName: Option[String], fieldName: String, filter: Option[InputFilter])
    extends AggFunction[Number, Number, Number](aggName.getOrElse(s"max_$fieldName")) {
  override def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: BaseVertexSteps,
      authContext: AuthContext
  ): Traversal[Number, Number] = {
    val property = PublicProperty.getPropertyTraversal(
      publicProperties,
      stepType,
      filter.fold(fromStep)(f => f.apply(db, publicProperties, stepType, fromStep, authContext)),
      fieldName,
      authContext
    )

    property
      .cast(UniMapping.int)
      .flatMap(_.max[JInt]().cast(UniMapping.jint))
      .orElse(property.cast(UniMapping.long).flatMap(_.max[JLong]().cast(UniMapping.jlong)))
      .orElse(property.cast(UniMapping.float).flatMap(_.max[JFloat]().cast(UniMapping.jfloat)))
      .orElse(property.cast(UniMapping.double).flatMap(_.max[JDouble]().cast(UniMapping.jdouble)))
      .getOrElse(throw BadRequestError(s"Property $fieldName in $fromStep can't be cast to number. Max aggregation is not applicable"))
      .asInstanceOf[Traversal[Number, Number]]
  }
  override def output(t: Number): Output[Number] = Output(t)(Writes(v => Json.obj(name -> v)))
}

case class AggCount(aggName: Option[String], filter: Option[InputFilter]) extends GroupAggregation[Long, JLong, Long](aggName.getOrElse("count")) {
  override def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: BaseVertexSteps,
      authContext: AuthContext
  ): Traversal[Long, JLong]                  = filter.fold(fromStep)(f => f.apply(db, publicProperties, stepType, fromStep, authContext)).count
  override def output(t: Long): Output[Long] = Output(t, Json.obj(name -> t))

}
//case class AggTop[T](fieldName: String) extends AggFunction[T](s"top_$fieldName")

case class FieldAggregation(aggName: Option[String], fieldName: String, orders: Seq[String], size: Option[Long], subAggs: Seq[Aggregation[_, _, _]])
    extends GroupAggregation[JList[JCollection[Any]], JList[JCollection[Any]], Map[Any, Map[String, Any]]](aggName.getOrElse(s"field_$fieldName")) {
  lazy val logger: Logger = Logger(getClass)
  override def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: BaseVertexSteps,
      authContext: AuthContext
  ): Traversal[JList[JCollection[Any]], JList[JCollection[Any]]] = {
    val elementLabel = StepLabel[Vertex]()
    val groupedVertices: Traversal[JMap.Entry[Any, JCollection[Any]], JMap.Entry[Any, JCollection[Any]]] =
      PublicProperty
        .getPropertyTraversal(publicProperties, stepType, fromStep.as(elementLabel), fieldName, authContext)
        .group(By(), By(__.select(elementLabel).fold()))
        .unfold[JMap.Entry[Any, JCollection[Any]]](null) // Map.Entry[K, List[V]]

    val sortedAndGroupedVertex = orders
      .map {
        case order if order.headOption.contains('-') => order.tail -> Order.desc
        case order if order.headOption.contains('+') => order.tail -> Order.asc
        case order                                   => order      -> Order.asc
      }
      .foldLeft(groupedVertices) {
        case (acc, (field, order)) if field == fieldName => acc.sort(By(__[JMap.Entry[Any, JCollection[Any]]].selectKeys, order))
        case (acc, (field, order)) if field == "count"   => acc.sort(By(__[JMap.Entry[Any, JCollection[Any]]].selectValues.count(Scope.local), order))
        case (acc, (field, _)) =>
          logger.warn(s"In field aggregation you can only sort by the field ($fieldName) or by count, not by $field")
          acc
      }

    val sizedSortedAndGroupedVertex = size.fold(sortedAndGroupedVertex)(sortedAndGroupedVertex.range(0, _))

    sizedSortedAndGroupedVertex
      .project[Any](
        By(__[JMap.Entry[Any, Any]].selectKeys) +: subAggs
          .map(a =>
            By(
              a.apply(
                  db,
                  publicProperties,
                  stepType,
                  fromStep.newInstance(__[JMap[Any, Any]].selectValues.unfold()),
                  authContext
                )
                .raw
            )
          ): _*
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
            .asInstanceOf[Seq[Aggregation[Any, Any, Any]]]
            .zip(e.tail)
            .map { case (a, r) => a.name -> a.output(r).toValue }
            .toMap
          val jsValues =
            subAggs
              .asInstanceOf[Seq[Aggregation[Any, Any, Any]]]
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

    val native: Map[Any, Map[String, Any]] = subMap.map { case (k, v) => k -> v.toValue }
    val json: JsObject                     = JsObject(subMap.map { case (k, v) => k.toString -> v.toJson })
    //Json.obj(name -> JsObject(subMap.map { case (k, v) ⇒ k.toString -> v.toJson }))
    Output(native, json)
  }
}

case class CategoryAggregation() // Map[String,
case class TimeAggregation(aggName: Option[String], fieldName: String, interval: Long, unit: ChronoUnit, subAggs: Seq[Aggregation[_, _, _]])
    extends GroupAggregation[JList[JCollection[Any]], JList[JCollection[Any]], Map[Any, Map[String, Any]]](aggName.getOrElse(s"time_$fieldName")) {
  val calendar: Calendar = Calendar.getInstance()

  def dateToKey(date: Date): Long =
    unit match {
      case ChronoUnit.WEEKS =>
        calendar.setTime(date)
        val year = calendar.get(Calendar.YEAR)
        val week = (calendar.get(Calendar.WEEK_OF_YEAR) / interval) * interval
        calendar.setTimeInMillis(0)
        calendar.set(Calendar.YEAR, year)
        calendar.set(Calendar.WEEK_OF_YEAR, week.toInt)
        calendar.getTimeInMillis

      case ChronoUnit.MONTHS =>
        calendar.setTime(date)
        val year  = calendar.get(Calendar.YEAR)
        val month = (calendar.get(Calendar.MONTH) / interval) * interval
        calendar.setTimeInMillis(0)
        calendar.set(Calendar.YEAR, year)
        calendar.set(Calendar.MONTH, month.toInt)
        calendar.getTimeInMillis

      case ChronoUnit.YEARS =>
        calendar.setTime(date)
        val year = (calendar.get(Calendar.YEAR) / interval) * interval
        calendar.setTimeInMillis(0)
        calendar.set(Calendar.YEAR, year.toInt)
        calendar.getTimeInMillis
      case other =>
        val duration = other.getDuration.toMillis * interval
        (date.getTime / duration) * duration
    }

  def keyToDate(key: Long): Date = new Date(key)

  override def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: BaseVertexSteps,
      authContext: AuthContext
  ): Traversal[JList[JCollection[Any]], JList[JCollection[Any]]] = {
    val elementLabel = StepLabel[Vertex]()
    val groupedVertices = PublicProperty
      .getPropertyTraversal(publicProperties, stepType, fromStep.as(elementLabel), fieldName, authContext)
      .map(date => dateToKey(date.asInstanceOf[Date]))
      .group(By[Long](), By(__.select(elementLabel).fold()))
      .unfold[JMap.Entry[Long, JCollection[Any]]](null) // Map.Entry[K, List[V]]

    groupedVertices
      .project[Any](
        By(__[JMap.Entry[Any, Any]].selectKeys)
          +: subAggs
            .map(a =>
              By(
                a.apply(db, publicProperties, stepType, fromStep.newInstance(__[JMap[Long, Any]].selectValues.unfold()), authContext).raw
              )
            ): _*
      )
      .fold
  }

  override def output(l: JList[JCollection[Any]]): Output[Map[Any, Map[String, Any]]] = {
    val subMap: Map[Date, Output[Map[String, Any]]] = l
      .asScala
      .map(_.asScala)
      .map { e =>
        val key = e.head match {
          case l: Long => keyToDate(l)
          case _       => new Date(0)
        }
        val values = subAggs
          .asInstanceOf[Seq[Aggregation[Any, Any, Any]]]
          .zip(e.tail)
          .map { case (a, r) => a.name -> a.output(r).toValue }
          .toMap
        val jsValues =
          subAggs
            .asInstanceOf[Seq[Aggregation[Any, Any, Any]]]
            .zip(e.tail)
            .foldLeft(JsObject.empty) {
              case (acc, (ar, r)) =>
                ar.output(r).toJson match {
                  case o: JsObject => acc ++ o
                  case v           => acc + (ar.name -> v)
                }
            }
        key -> Output(values, jsValues)
      }
      .toMap

    val native: Map[Any, Map[String, Any]] = subMap.map { case (k, v) => k -> v.toValue }
    val json: JsObject                     = JsObject(subMap.map { case (k, v) => k.getTime.toString -> Json.obj(fieldName -> v.toJson) })
    Output(native, json)
  }

}
