package org.thp.scalligraph.query

import java.lang.{Long => JLong}
import java.time.temporal.ChronoUnit
import java.util.{Calendar, Date, Collection => JCollection, List => JList, Map => JMap}

import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.thp.scalligraph.BadRequestError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.Mapping
import org.thp.scalligraph.steps.Converter.{CMap, CMapEntry}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps._
import play.api.Logger
import play.api.libs.json.JsObject

import scala.reflect.runtime.{universe => ru}

//object GroupAggregation {
//
//  object AggObj {
//    def unapply(field: Field): Option[(String, FObject)] = field match {
//      case f: FObject =>
//        f.get("_agg") match {
//          case FString(name) => Some(name -> (f - "_agg"))
//          case _             => None
//        }
//      case _ => None
//    }
//  }
//
//  val intervalParser: FieldsParser[(Long, ChronoUnit)] = FieldsParser[(Long, ChronoUnit)]("interval") {
//    case (_, f) =>
//      withGood(
//        FieldsParser.long.optional.on("_interval")(f),
//        FieldsParser[ChronoUnit]("chronoUnit") {
//          case (_, f @ FString(value)) =>
//            Or.from(
//              Try(ChronoUnit.valueOf(value)).toOption,
//              One(InvalidFormatAttributeError("_unit", "chronoUnit", ChronoUnit.values.toSet.map((_: ChronoUnit).toString), f))
//            )
//        }.on("_unit")(f)
//      )((i, u) => i.getOrElse(0L) -> u)
//  }
//
//  val intervalRegex: Regex = "(\\d+)([smhdwMy])".r
//
//  val mergedIntervalParser: FieldsParser[(Long, ChronoUnit)] = FieldsParser[(Long, ChronoUnit)]("interval") {
//    case (_, FString(intervalRegex(interval, unit))) =>
//      Good(unit match {
//        case "s" => interval.toLong -> ChronoUnit.SECONDS
//        case "m" => interval.toLong -> ChronoUnit.MINUTES
//        case "h" => interval.toLong -> ChronoUnit.HOURS
//        case "d" => interval.toLong -> ChronoUnit.DAYS
//        case "w" => interval.toLong -> ChronoUnit.WEEKS
//        case "M" => interval.toLong -> ChronoUnit.MONTHS
//        case "y" => interval.toLong -> ChronoUnit.YEARS
//      })
//  }
//
//  def groupAggregationParser: PartialFunction[String, FieldsParser[GroupAggregation[_]]] = {
//    case "field" =>
//      FieldsParser("FieldAggregation") {
//        case (_, field) =>
//          withGood(
//            FieldsParser.string.optional.on("_name")(field),
//            FieldsParser.string.on("_field")(field),
//            FieldsParser.string.sequence.on("_order")(field).orElse(FieldsParser.string.on("_order").map("order")(Seq(_))(field)),
//            FieldsParser.long.optional.on("_size")(field),
//            aggregationFieldsParser.sequence.on("_select")(field)
//          )((aggName, fieldName, order, size, subAgg) => FieldAggregation(aggName, fieldName, order, size, subAgg))
//      }
//    case "count" =>
//      FieldsParser("CountAggregation") {
//        case (_, field) => FieldsParser.string.optional.on("_name")(field).map(aggName => AggCount(aggName))
//      }
//    case "time" =>
//      FieldsParser("TimeAggregation") {
//        case (_, field) =>
//          withGood(
//            FieldsParser.string.optional.on("_name")(field),
//            FieldsParser.string.sequence.on("_fields")(field),
//            mergedIntervalParser.on("_interval").orElse(intervalParser)(field),
//            aggregationFieldsParser.sequence.on("_select")(field)
//          )((aggName, fieldNames, intervalUnit, subAgg) => TimeAggregation(aggName, fieldNames.head, intervalUnit._1, intervalUnit._2, subAgg))
//      }
//  }
//
//  def functionAggregationParser: PartialFunction[String, FieldsParser[Aggregation[_]]] = {
//    case "avg" =>
//      FieldsParser("AvgAggregation") {
//        case (_, field) =>
//          withGood(
//            FieldsParser.string.optional.on("_name")(field),
//            FieldsParser.string.on("_field")(field)
//          )((aggName, fieldName) => AggAvg(aggName, fieldName))
//      }
//    case "min" =>
//      FieldsParser("MinAggregation") {
//        case (_, field) =>
//          withGood(
//            FieldsParser.string.optional.on("_name")(field),
//            FieldsParser.string.on("_field")(field)
//          )((aggName, fieldName) => AggMin(aggName, fieldName))
//      }
//    case "count" =>
//      FieldsParser("CountAggregation") {
//        case (_, field) => FieldsParser.string.optional.on("_name")(field).map(aggName => AggCount(aggName))
//      }
//  }
//
//  val aggregationFieldsParser: FieldsParser[Aggregation[_]] = {
//    def fp(name: String) =
//      functionAggregationParser
//        .orElse(groupAggregationParser)
//        .applyOrElse(
//          name,
//          (name: String) =>
//            FieldsParser[Aggregation[_]]("aggregation") {
//              case _ => Bad(One(InvalidFormatAttributeError("_agg", "aggregation name", Set("avg", "min", "max", "count", "top"), FString(name))))
//            }
//        )
//    FieldsParser("aggregation") {
//      case (_, AggObj(name, field)) => fp(name)(field)
//      //    case other => Bad(InvalidFormatAttributeError()) // TODO
//    }
//  }
//
//  implicit val fieldsParser: FieldsParser[GroupAggregation[_]] = FieldsParser("aggregation") {
//    case (_, AggObj(name, field)) => groupAggregationParser(name)(field)
//    //    orElse Bad(InvalidFormatAttributeError()) // TODO
//  }
//}

abstract class Aggregation[TD, TG](val name: String) {

  def getProperty[PD, PG](
      properties: Seq[PublicProperty[_, _, _]],
      traversalType: ru.Type,
      fieldName: String,
      authContext: AuthContext
  ): PublicProperty[PD, PG, Converter[PD, PG]] = {
    val path = FPath(fieldName)
    properties
      .iterator
      .collectFirst {
        case p if p.traversalType =:= traversalType && path.startsWith(p.propertyPath).isDefined =>
          p.asInstanceOf[PublicProperty[PD, PG, Converter[PD, PG]]]
      }
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $traversalType not found"))
  }

  def apply[D, G, PD, PG](
      publicProperties: List[PublicProperty[_, _, _]],
      traversalType: ru.Type,
      fromStep: Traversal[D, G, Converter[D, G]],
      authContext: AuthContext
  ): Traversal[Output[TD], TG, Converter[Output[TD], TG]]
}

abstract class GroupAggregation(name: String) extends Aggregation(name)

abstract class AggFunction(name: String) extends Aggregation(name) {
//  implicit val numberWrites: Writes[Number] = Writes[Number] {
//    case i: JInt    => JsNumber(i.toInt)
//    case l: JLong   => JsNumber(l.toLong)
//    case f: JFloat  => JsNumber(f.toDouble)
//    case d: JDouble => JsNumber(d.doubleValue())
//    case o: Number  => JsNumber(o.doubleValue())
//  }
}

case class AggSum(aggName: Option[String], fieldName: String) extends AggFunction(s"sum_$fieldName") {
  override def apply[D, G, TD, TG](
      publicProperties: List[PublicProperty[_, _, _]],
      traversalType: ru.Type,
      fromStep: Traversal[D, G, Converter[D, G]],
      authContext: AuthContext
  ): Traversal[Output[TD], TG, Converter[Output[TD], TG]] = {
    val property: PublicProperty[TD, TG, _] = getProperty[TD, TG](publicProperties, traversalType, fieldName, authContext)
    property.get(fromStep, FPath(fieldName)).mapAsNumber(_.sum).map(property.mapping.toOutput)
  }

//  override def toOutput[D, G, C <: Converter[D, G]](
//      publicProperties: List[PublicProperty[_, _, _]],
//      traversalType: ru.Type,
//      fromStep: Traversal[D, G, C],
//      authContext: AuthContext
//  ): Output[PD] = {
//    val property = getProperty(publicProperties, traversalType, fieldName, authContext)
//    val value    = property.get(fromStep, FPath(fieldName)).mapAsNumber(_.sum).head()
//    property.mapping.toOutput(value)
////    Output(value)(property.)
//  }
}
//
//case class AggAvg[D](aggName: Option[String], fieldName: String) extends AggFunction[D, Number, Number](s"avg_$fieldName") {
//  override def apply(
//      publicProperties: List[PublicProperty[_, _]],
//      traversalType: ru.Type,
//      fromStep: UntypedTraversal,
//      authContext: AuthContext
//  ): Traversal[Number, Number] =
//    getPropertyTraversal(publicProperties, traversalType, fromStep, fieldName, authContext).asNumber.get.mean
//
//  override def output(avg: Number): Output[Number] = Output(avg) // TODO add aggregation name
//}
//
case class AggMin(aggName: Option[String], fieldName: String) extends AggFunction(s"min_$fieldName") {
  override def apply[D, G, TD, TG](
      publicProperties: List[PublicProperty[_, _, _]],
      traversalType: ru.Type,
      fromStep: Traversal[D, G, Converter[D, G]],
      authContext: AuthContext
  ): Traversal[Output[TD], TG, Converter[Output[TD], TG]] = {
    val property = getProperty[TD, TG](publicProperties, traversalType, fieldName, authContext)
    property.get(fromStep, FPath(fieldName)).mapAsNumber(_.min).map(property.mapping.toOutput)
  }
}
//
//case class AggMax[D](aggName: Option[String], fieldName: String) extends AggFunction[D, Number, Number](s"avg_$fieldName") {
//  override def apply(
//      publicProperties: List[PublicProperty[_, _]],
//      traversalType: ru.Type,
//      fromStep: UntypedTraversal,
//      authContext: AuthContext
//  ): Traversal[Number, _] =
//    getPropertyTraversal(publicProperties, traversalType, fromStep, fieldName, authContext).asNumber.get.mean
//
//  override def output(number: Number): Output[Number] = Output(number) // TODO add aggregation name
//}
//
//case class AggCount(aggName: Option[String]) extends GroupAggregation[Long](aggName.getOrElse("count")) {
//  override def apply(
//      publicProperties: List[PublicProperty[_, _]],
//      traversalType: ru.Type,
//      fromStep: UntypedTraversal,
//      authContext: AuthContext
//  ): UntypedTraversal = fromStep.count
//
//  override def output(t: Any): Output[Long] = Output(t.asInstanceOf[Long])(Writes(v => Json.obj(name -> v)))
//
//}
//case class AggTop[T](fieldName: String) extends AggFunction[T](s"top_$fieldName")

case class FieldAggregation(
    aggName: Option[String],
    fieldName: String,
    orders: Seq[String],
    size: Option[Long],
    subAggs: Seq[Aggregation]
) extends GroupAggregation(aggName.getOrElse(s"field_$fieldName")) {
  lazy val logger: Logger = Logger(getClass)

  override def apply[D, G, TD, TG](
      publicProperties: List[PublicProperty[_, _, _]],
      traversalType: ru.Type,
      fromStep: Traversal[D, G, Converter[D, G]],
      authContext: AuthContext
  ): Traversal[Output[TD], TG, Converter[Output[TD], TG]] = {
//  ): Traversal[Output[Map[PD, Map[String, Any]]], JList[JMap[String, Any]], Converter[
//    Output[Map[PD, Map[String, Any]]],
//    JList[JMap[String, Any]]
//  ]] = {
    val property        = getProperty[TD, TG](publicProperties, traversalType, fieldName, authContext)
    val groupedVertices = fromStep.group(_.by(property.get(_, FPath(fieldName)))).unfold

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

    def subAggProjection[X, Y]: Seq[GenericBySelector[D, G, Converter[D, G]] => ByResult[G, Output[X], Y, Converter[Output[X], Y]]] = subAggs.map {
      agg => (s: GenericBySelector[D, G, Converter[D, G]]) =>
        s.by(t => agg[D, G, X, Y](publicProperties, traversalType, t, authContext))
    }

    sizedSortedAndGroupedVertex
      .project(
        _.by(_.selectKeys)
          .by(
            _.selectValues
              .unfold
              .flatProject(subAggProjection: _*)
              .map { aggResult =>
                val outputs = subAggs.zip(aggResult.asInstanceOf[Seq[Output[_]]])
                Output(outputs.map(kv => kv._1.name -> kv._2.toValue).toMap, JsObject(outputs.map(kv => kv._1.name -> kv._2.toJson)))
              }
          )
      )
      .fold
      .map(x => Output(x.map(kv => kv._1 -> kv._2.toValue).toMap, JsObject(x.map(kv => kv._1.toString -> kv._2.toJson))))
  }
}

case class CategoryAggregation() // Map[String,
case class TimeAggregation[PD, PC <: Converter[PD, Date]](
    aggName: Option[String],
    fieldName: String,
    interval: Long,
    unit: ChronoUnit,
    subAggs: Seq[Aggregation[_, _, _, _, _, _]]
) extends GroupAggregation[PD, Date, PC, Map[Long, Map[String, Any]], JList[JMap[String, Any]], Converter[
      Output[Map[Long, Map[String, Any]]],
      JList[JMap[String, Any]]
    ]](
      aggName.getOrElse(s"time_$fieldName")
    ) {
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

  override def apply[D, G, C <: Converter[D, G]](
      publicProperties: List[PublicProperty[_, _, _]],
      traversalType: ru.Type,
      fromStep: Traversal[D, G, C],
      authContext: AuthContext
  ): Traversal[Output[Map[Long, Map[String, Any]]], JList[JMap[String, Any]], Converter[Output[Map[Long, Map[String, Any]]], JList[
    JMap[String, Any]
  ]]] = {
    val property: PublicProperty[PD, Date, PC] = getProperty(publicProperties, traversalType, fieldName, authContext)
    val groupedVertex: Traversal[Map[Long, Seq[D]], JMap[JLong, JCollection[G]], CMap[Long, D, JLong, G, Converter[Long, JLong], C]] =
      fromStep.group(_.by(t => property.get(t, FPath(fieldName)).graphMap[Long, JLong, Converter[Long, JLong]](dateToKey, _ => Converter.long)))

    def subAggProjection[X, Y]: Seq[GenericBySelector[D, G, C] => ByResult[G, Output[X], Y, Converter[Output[X], Y]]] = subAggs.map {
      a => (s: GenericBySelector[D, G, C]) =>
        s.by(t => a.asInstanceOf[Aggregation[D, G, C, X, Y, Converter[Output[X], Y]]].apply(publicProperties, traversalType, t, authContext))
    }

    groupedVertex
      .project(
        _.by(_.selectKeys)
          .by(
            _.selectValues
              .unfold
              .flatProject(subAggProjection: _*)
              .map { aggResult =>
                val outputs = subAggs.zip(aggResult.asInstanceOf[Seq[Output[_]]])
                Output(outputs.map(kv => kv._1.name -> kv._2.toValue).toMap, JsObject(outputs.map(kv => kv._1.name -> kv._2.toJson)))
              }
          )
      )
      .fold
      .map(x => Output(x.map(kv => kv._1 -> kv._2.toValue).toMap, JsObject(x.map(kv => kv._1.toString -> kv._2.toJson))))
  }
}

//    val elementLabel                         = UUID.randomUUID().toString
//
//    val groupedVertices = PublicProperty
//      .getPropertyTraversal(publicProperties, traversalType, fromStep.as(elementLabel), fieldName, authContext)
//      .map[Date, Long](dateToKey)
//      .group(_.by, _.by(_.select(elementLabel).fold))
//      .unfold // Map.Entry[K, List[V]]
//
//    groupedVertices
//      .project(
//        ((_: UntypedBySelector).by(_.selectKeys)) +: subAggs
//          .map(agg => (_: UntypedBySelector).by(t => agg(publicProperties, traversalType, t.selectValues.unfold, authContext))): _*
//      )
//      .fold
//  }
//
//  override def output(l: Any): Output[Map[Any, Map[String, Any]]] = {
//    val subMap: Map[Date, Output[Map[String, Any]]] = l
//      .asInstanceOf[JList[JCollection[Any]]]
//      .asScala
//      .map(_.asScala)
//      .map { e =>
//        val key = e.head match {
//          case l: Long => keyToDate(l)
//          case _       => new Date(0)
//        }
//        val values = subAggs
//          .asInstanceOf[Seq[Aggregation[Any]]]
//          .zip(e.tail)
//          .map { case (a, r) => a.name -> a.output(r).toValue }
//          .toMap
//        val jsValues =
//          subAggs
//            .asInstanceOf[Seq[Aggregation[Any]]]
//            .zip(e.tail)
//            .foldLeft(JsObject.empty) {
//              case (acc, (ar, r)) =>
//                ar.output(r).toJson match {
//                  case o: JsObject => acc ++ o
//                  case v           => acc + (ar.name -> v)
//                }
//            }
//        key -> Output(values, jsValues)
//      }
//      .toMap
//
//    val native: Map[Any, Map[String, Any]] = subMap.map { case (k, v) => k -> v.toValue }
//    val json: JsObject                     = JsObject(subMap.map { case (k, v) => k.getTime.toString -> Json.obj(fieldName -> v.toJson) })
//    Output(native, json)
//  }
//
//}
