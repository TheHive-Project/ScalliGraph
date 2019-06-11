package org.thp.scalligraph.controllers

import java.util.Date

import scala.language.experimental.macros
import scala.util.Try

import play.api.libs.json.JsValue

import org.scalactic.Accumulation._
import org.scalactic._
import org.thp.scalligraph._
import org.thp.scalligraph.auth.Permission
import org.thp.scalligraph.macros.FieldsParserMacro
import org.thp.scalligraph.query.{PropertyUpdater, PublicProperty}

class FieldsParser[T](
    val formatName: String,
    val acceptedInput: Set[String],
    val parse: PartialFunction[(FPath, Field), T Or Every[AttributeError]]
) {

  def apply(path: FPath, field: Field): T Or Every[AttributeError] =
    parse.lift((path, field)).getOrElse {
      if (field == FUndefined) Bad(One(MissingAttributeError(path.toString)))
      else
        Bad(One(InvalidFormatAttributeError(path.toString, formatName, acceptedInput, field)))
    }

  def apply(field: Field): T Or Every[AttributeError] =
    apply(FPath.empty, field)

  def on(pathElement: String): FieldsParser[T] =
    new FieldsParser[T](formatName, acceptedInput.map(pathElement + "/" + _), {
      case (path, field) ⇒ apply(path :/ pathElement, field.get(pathElement))
    })

  def andThen[U, R](nextFormatName: String)(fp: FieldsParser[U])(f: (U, T) ⇒ R): FieldsParser[R] =
    new FieldsParser[R](
      s"$formatName&$nextFormatName",
      acceptedInput ++ fp.acceptedInput, {
        case (path, field) ⇒
          val value1 = apply(path, field)
          val value2 = fp(path, field)
          withGood(value2, value1)(f)
      }
    )

  def orElse[U >: T](fp: FieldsParser[U]): FieldsParser[U] =
    new FieldsParser[U](formatName, acceptedInput ++ fp.acceptedInput, parse orElse fp.parse)

  def map[U](newFormatName: String)(f: T ⇒ U): FieldsParser[U] =
    new FieldsParser(newFormatName, acceptedInput, {
      case (path, fields) ⇒ apply(path, fields).map(f)
    })

  def flatMap[U](newFormatName: String)(fp: FieldsParser[U])(implicit ev: T <:< Field): FieldsParser[U] =
    new FieldsParser(newFormatName, acceptedInput, {
      case (path, fields) ⇒ apply(path, fields).flatMap(f ⇒ fp(ev(f)))
    })

  def sequence: FieldsParser[Seq[T]] =
    new FieldsParser[Seq[T]](
      s"seq($formatName)",
      acceptedInput.map(i ⇒ s"[$i]"), {
        case (path, field) ⇒
          field match {
            case FSeq(subFields) ⇒
              subFields
                .zipWithIndex
                .validatedBy { case (f, i) ⇒ apply(path.toSeq(i), f) }
            case FNull | FUndefined ⇒ Good(Nil)
            case other ⇒
              Bad(One(InvalidFormatAttributeError(path.toString, "object", Set(s"[$formatName]"), other)))
          }
      }
    )

  def set: FieldsParser[Set[T]] = sequence.map(s"set($formatName)")(_.toSet)

  def optional: FieldsParser[Option[T]] =
    new FieldsParser[Option[T]](
      s"option($formatName)",
      acceptedInput.map(i ⇒ s"$i?"), {
        case (path, field) ⇒
          field match {
            case FNull | FUndefined ⇒ Good(None)
            case _                  ⇒ apply(path, field).map(Some(_))
          }
      }
    )

  def toUpdate: UpdateFieldsParser[T] = new UpdateFieldsParser[T](formatName, Seq(FPathEmpty → this))
}

trait FieldsParserLowPrio {
  implicit def build[T]: FieldsParser[T] = macro FieldsParserMacro.getOrBuildFieldsParser[T]
}

object FieldsParser extends FieldsParserLowPrio {
  def apply[T](implicit fp: FieldsParser[T]): FieldsParser[T] = fp

  def apply[T](formatName: String, acceptedInput: Set[String])(parse: PartialFunction[(FPath, Field), T Or Every[AttributeError]]) =
    new FieldsParser[T](formatName, acceptedInput, parse)

  def apply[T](formatName: String)(parse: PartialFunction[(FPath, Field), T Or Every[AttributeError]]) =
    new FieldsParser[T](formatName, Set(formatName), parse)

  def update(name: String, properties: Seq[PublicProperty[_, _]]): FieldsParser[Seq[PropertyUpdater]] =
    FieldsParser(name) {
      case (_, FObject(fields)) ⇒
        fields
          .map {
            case (k, v) ⇒ FPath(k) → v
          }
          .flatMap {
            case (FPathElem(propertyName, path), value) ⇒
              properties
                .find(_.propertyName == propertyName)
                .flatMap(_.updateFieldsParser)
                .map(_.apply(path, value).badMap(_.map(_.withName(propertyName))))
            case _ ⇒ None
          }
          .toSeq
          .combined
    }

  def good[T](value: T): FieldsParser[T] =
    new FieldsParser[T]("good", Set.empty, {
      case _ ⇒ Good(value)
    })
  def empty[T]: FieldsParser[T] = new FieldsParser[T]("empty", Set.empty, PartialFunction.empty)

  private def unlift[T, R](f: T ⇒ Option[R]): PartialFunction[T, R] =
    new PartialFunction[T, R] {
      def apply(x: T): R               = f(x).get
      def isDefinedAt(x: T): Boolean   = f(x).isDefined
      override def lift: T ⇒ Option[R] = f
    }

  implicit val file: FieldsParser[FFile] = FieldsParser[FFile]("file") {
    case (_, f: FFile) ⇒ Good(f)
  }
  implicit val string: FieldsParser[String] = FieldsParser[String]("string") {
    case (_, FString(value)) ⇒ Good(value)
    case (_, FAny(Seq(s)))   ⇒ Good(s)
  }
  implicit val int: FieldsParser[Int] = FieldsParser[Int]("int")(unlift {
    case (_, FNumber(n))   ⇒ Some(Good(n.toInt))
    case (_, FAny(Seq(s))) ⇒ Try(Good(s.toInt)).toOption
    case _                 ⇒ None
  })
  implicit val long: FieldsParser[Long] = FieldsParser[Long]("long")(unlift {
    case (_, FNumber(n))   ⇒ Some(Good(n.toLong))
    case (_, FAny(Seq(s))) ⇒ Try(Good(s.toLong)).toOption
    case _                 ⇒ None
  })
  implicit val boolean: FieldsParser[Boolean] =
    FieldsParser[Boolean]("boolean")(unlift {
      case (_, FBoolean(b))  ⇒ Some(Good(b))
      case (_, FAny(Seq(s))) ⇒ Try(Good(s.toBoolean)).toOption
      case _                 ⇒ None
    })
  implicit val date: FieldsParser[Date] = FieldsParser[Date]("date")(unlift {
    case (_, FNumber(n))   ⇒ Some(Good(new Date(n)))
    case (_, FAny(Seq(s))) ⇒ Try(Good(new Date(s.toLong))).toOption
    case _                 ⇒ None
  })
  implicit val float: FieldsParser[Float] = FieldsParser[Float]("float")(unlift {
    case (_, FNumber(n))   ⇒ Some(Good(n.toFloat))
    case (_, FAny(Seq(s))) ⇒ Try(Good(s.toFloat)).toOption
    case _                 ⇒ None
  })
  implicit val double: FieldsParser[Double] = FieldsParser[Double]("float")(unlift {
    case (_, FNumber(n))   ⇒ Some(Good(n.toDouble))
    case (_, FAny(Seq(s))) ⇒ Try(Good(s.toDouble)).toOption
    case _                 ⇒ None
  })
  implicit val json: FieldsParser[JsValue] = FieldsParser[JsValue]("json") {
    case (_, _field) ⇒ Good(_field.toJson)
  }
  implicit def seq[A](implicit fp: FieldsParser[A]): FieldsParser[Seq[A]]       = fp.sequence
  implicit def set[A](implicit fp: FieldsParser[A]): FieldsParser[Set[A]]       = fp.set
  implicit def option[A](implicit fp: FieldsParser[A]): FieldsParser[Option[A]] = fp.optional
  implicit val permission: FieldsParser[Permission]                             = FieldsParser.string.asInstanceOf[FieldsParser[Permission]]
}