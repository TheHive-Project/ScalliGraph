package org.thp.scalligraph.query

import java.lang.{Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.time.temporal.ChronoUnit
import java.util.{Calendar, Date, UUID, Collection => JCollection, List => JList}

import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.scalactic.Accumulation.withGood
import org.scalactic.{Bad, Good, One, Or}
import org.thp.scalligraph.InvalidFormatAttributeError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{UntypedBySelector, UntypedTraversal}
import play.api.Logger
import play.api.libs.json.{JsNumber, JsObject, Json, Writes}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
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

  def groupAggregationParser: PartialFunction[String, FieldsParser[GroupAggregation[_]]] = {
    case "field" =>
      FieldsParser("FieldAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            FieldsParser.string.on("_field")(field),
            FieldsParser.string.sequence.on("_order")(field).orElse(FieldsParser.string.on("_order").map("order")(Seq(_))(field)),
            FieldsParser.long.optional.on("_size")(field),
            aggregationFieldsParser.sequence.on("_select")(field)
          )((aggName, fieldName, order, size, subAgg) => FieldAggregation(aggName, fieldName, order, size, subAgg))
      }
    case "count" =>
      FieldsParser("CountAggregation") {
        case (_, field) => FieldsParser.string.optional.on("_name")(field).map(aggName => AggCount(aggName))
      }
    case "time" =>
      FieldsParser("TimeAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            FieldsParser.string.sequence.on("_fields")(field),
            mergedIntervalParser.on("_interval").orElse(intervalParser)(field),
            aggregationFieldsParser.sequence.on("_select")(field)
          )((aggName, fieldNames, intervalUnit, subAgg) => TimeAggregation(aggName, fieldNames.head, intervalUnit._1, intervalUnit._2, subAgg))
      }
  }

  def functionAggregationParser: PartialFunction[String, FieldsParser[Aggregation[_]]] = {
    case "avg" =>
      FieldsParser("AvgAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            FieldsParser.string.on("_field")(field)
          )((aggName, fieldName) => AggAvg(aggName, fieldName))
      }
    case "min" =>
      FieldsParser("MinAggregation") {
        case (_, field) =>
          withGood(
            FieldsParser.string.optional.on("_name")(field),
            FieldsParser.string.on("_field")(field)
          )((aggName, fieldName) => AggMin(aggName, fieldName))
      }
    case "count" =>
      FieldsParser("CountAggregation") {
        case (_, field) => FieldsParser.string.optional.on("_name")(field).map(aggName => AggCount(aggName))
      }
  }

  val aggregationFieldsParser: FieldsParser[Aggregation[_]] = {
    def fp(name: String) =
      functionAggregationParser
        .orElse(groupAggregationParser)
        .applyOrElse(
          name,
          (name: String) =>
            FieldsParser[Aggregation[_]]("aggregation") {
              case _ => Bad(One(InvalidFormatAttributeError("_agg", "aggregation name", Set("avg", "min", "max", "count", "top"), FString(name))))
            }
        )
    FieldsParser("aggregation") {
      case (_, AggObj(name, field)) => fp(name)(field)
      //    case other => Bad(InvalidFormatAttributeError()) // TODO
    }
  }

  implicit val fieldsParser: FieldsParser[GroupAggregation[_]] = FieldsParser("aggregation") {
    case (_, AggObj(name, field)) => groupAggregationParser(name)(field)
    //    orElse Bad(InvalidFormatAttributeError()) // TODO
  }
}

abstract class GroupAggregation[R: ClassTag](name: String) extends Aggregation[R](name) {

  def get(
      properties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: UntypedTraversal,
      authContext: AuthContext
  ): Output[R] =
    output(apply(properties, stepType, fromStep, authContext).as[R, R].head())
}

abstract class Aggregation[R](val name: String) {

  def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: UntypedTraversal,
      authContext: AuthContext
  ): UntypedTraversal

  def output(t: Any): Output[R]
}

abstract class AggFunction[R](name: String) extends Aggregation[R](name) {
  implicit val numberWrites: Writes[Number] = Writes[Number] {
    case i: JInt    => JsNumber(i.toInt)
    case l: JLong   => JsNumber(l.toLong)
    case f: JFloat  => JsNumber(f.toDouble)
    case d: JDouble => JsNumber(d.doubleValue())
    case o: Number  => JsNumber(o.doubleValue())
  }
}

case class AggSum(fieldName: String) extends AggFunction[Number](s"sum_$fieldName") {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: UntypedTraversal,
      authContext: AuthContext
  ): UntypedTraversal =
    PublicProperty.getPropertyTraversal(publicProperties, stepType, fromStep, fieldName, authContext).sum

  override def output(t: Any): Output[Number] = Output(t.asInstanceOf[Number]) // TODO add aggregation name
}

case class AggAvg(aggName: Option[String], fieldName: String) extends AggFunction[Number](aggName.getOrElse(s"avg_$fieldName")) {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: UntypedTraversal,
      authContext: AuthContext
  ): UntypedTraversal =
    PublicProperty.getPropertyTraversal(publicProperties, stepType, fromStep, fieldName, authContext).mean

  override def output(t: Any): Output[Number] = Output(t.asInstanceOf[Number]) // TODO add aggregation name
}

case class AggMin(aggName: Option[String], fieldName: String) extends AggFunction[Number](aggName.getOrElse(s"min_$fieldName")) {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: UntypedTraversal,
      authContext: AuthContext
  ): UntypedTraversal =
    PublicProperty.getPropertyTraversal(publicProperties, stepType, fromStep, fieldName, authContext).min

  override def output(t: Any): Output[Number] = Output(t.asInstanceOf[Number])(Writes(v => Json.obj(name -> v)))
}

case class AggMax(fieldName: String) extends AggFunction[Number](s"max_$fieldName") {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: UntypedTraversal,
      authContext: AuthContext
  ): UntypedTraversal =
    PublicProperty.getPropertyTraversal(publicProperties, stepType, fromStep, fieldName, authContext).max

  override def output(t: Any): Output[Number] = Output(t.asInstanceOf[Number])(Writes(v => Json.obj(name -> v)))
}

case class AggCount(aggName: Option[String]) extends GroupAggregation[Long](aggName.getOrElse("count")) {
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: UntypedTraversal,
      authContext: AuthContext
  ): UntypedTraversal = fromStep.count

  override def output(t: Any): Output[Long] = Output(t.asInstanceOf[Long])(Writes(v => Json.obj(name -> v)))

}
//case class AggTop[T](fieldName: String) extends AggFunction[T](s"top_$fieldName")

case class FieldAggregation(aggName: Option[String], fieldName: String, orders: Seq[String], size: Option[Long], subAggs: Seq[Aggregation[_]])
    extends GroupAggregation[Map[Any, Map[String, Any]]](aggName.getOrElse(s"field_$fieldName")) {
  lazy val logger: Logger = Logger(getClass)
  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: UntypedTraversal,
      authContext: AuthContext
  ): UntypedTraversal = {
    val elementLabel = UUID.randomUUID().toString
    val groupedVertices: UntypedTraversal =
      PublicProperty
        .getPropertyTraversal(publicProperties, stepType, fromStep.as(elementLabel), fieldName, authContext)
        .group(_.by, _.by(_.select(elementLabel).fold))
        .unfold // Map.Entry[valueOf(fieldName), List[Vertex]]

    val sortedAndGroupedVertex = orders
      .map {
        case order if order.headOption.contains('-') => order.tail -> Order.desc
        case order if order.headOption.contains('+') => order.tail -> Order.asc
        case order                                   => order      -> Order.asc
      }
      .foldLeft(groupedVertices) {
        case (acc, (field, order)) if field == fieldName => acc.sort(_.by(_.selectKeys, order))
        case (acc, (field, order)) if field == "count"   => acc.sort(_.by(_.selectValues.localCount, order))
        case (acc, (field, _)) =>
          logger.warn(s"In field aggregation you can only sort by the field ($fieldName) or by count, not by $field")
          acc
      }

    val sizedSortedAndGroupedVertex = size.fold(sortedAndGroupedVertex)(sortedAndGroupedVertex.limit)
    sizedSortedAndGroupedVertex
      .project(
        ((_: UntypedBySelector).by(_.selectKeys)) +: subAggs
          .map(agg => (_: UntypedBySelector).by(t => agg(publicProperties, stepType, t.selectValues.unfold, authContext))): _*
      )
      .fold
  }

  override def output(l: Any): Output[Map[Any, Map[String, Any]]] = {
    val subMap: Map[Any, Output[Map[String, Any]]] = l
      .asInstanceOf[Seq[Seq[Any]]]
      .flatMap { e =>
        val key = e.head
        if (key != "") { // maybe should be compared with property.mapping.noValue but property is not accessible here
          val values = subAggs
            .asInstanceOf[Seq[Aggregation[Any]]]
            .zip(e.tail)
            .map { case (a, r) => a.name -> a.output(r).toValue }
            .toMap
          val jsValues =
            subAggs
              .asInstanceOf[Seq[Aggregation[Any]]]
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
case class TimeAggregation(aggName: Option[String], fieldName: String, interval: Long, unit: ChronoUnit, subAggs: Seq[Aggregation[_]])
    extends GroupAggregation[Map[Any, Map[String, Any]]](aggName.getOrElse(s"time_$fieldName")) {
  val calendar: Calendar = Calendar.getInstance()

  def dateToKey(date: Date): Long =
    unit match {
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
      // TODO week
      case other =>
        val duration = other.getDuration.toMillis * interval
        (date.getTime / duration) * duration
    }

  def keyToDate(key: Long): Date = new Date(key)

  override def apply(
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      fromStep: UntypedTraversal,
      authContext: AuthContext
  ): UntypedTraversal = {
    val elementLabel = UUID.randomUUID().toString
    val groupedVertices = PublicProperty
      .getPropertyTraversal(publicProperties, stepType, fromStep.as(elementLabel), fieldName, authContext)
      .map[Date, Long](dateToKey)
      .group(_.by, _.by(_.select(elementLabel).fold))
      .unfold // Map.Entry[K, List[V]]

    groupedVertices
      .project(
        ((_: UntypedBySelector).by(_.selectKeys)) +: subAggs
          .map(agg => (_: UntypedBySelector).by(t => agg(publicProperties, stepType, t.selectValues.unfold, authContext))): _*
      )
      .fold
  }

  override def output(l: Any): Output[Map[Any, Map[String, Any]]] = {
    val subMap: Map[Date, Output[Map[String, Any]]] = l
      .asInstanceOf[JList[JCollection[Any]]]
      .asScala
      .map(_.asScala)
      .map { e =>
        val key = e.head match {
          case l: Long => keyToDate(l)
          case _       => new Date(0)
        }
        val values = subAggs
          .asInstanceOf[Seq[Aggregation[Any]]]
          .zip(e.tail)
          .map { case (a, r) => a.name -> a.output(r).toValue }
          .toMap
        val jsValues =
          subAggs
            .asInstanceOf[Seq[Aggregation[Any]]]
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
