package org.thp.scalligraph.controllers

import java.util.Date

import org.scalactic.Accumulation._
import org.scalactic._
import org.thp.scalligraph.macros.FieldsParserMacro
import org.thp.scalligraph.{AttributeError, FPath, InvalidFormatAttributeError, MissingAttributeError, UnknownAttributeError}

import scala.language.experimental.macros
import scala.util.Try

abstract class BaseFieldsParser[+T] {
  bfp ⇒

  val formatName: String

  def apply(field: Field): T Or Every[AttributeError]

  def andThen[U, R](nextFormatName: String)(fp: BaseFieldsParser[U])(f: (U, T) ⇒ R): BaseFieldsParser[R] =
    new BaseFieldsParser[R] {
      val formatName = s"${bfp.formatName}&${fp.formatName}"
      def apply(field: Field): R Or Every[AttributeError] =
        withGood(fp(field), bfp(field))(f)
    }
}

object BaseFieldsParser {
  def good[T](value: T): FieldsParser[T] = FieldsParser[T]("empty", Nil) {
    case _ ⇒ Good(value)
  }
}

case class UpdateFieldsParser[T](formatName: String, parsers: Map[FPath, FieldsParser[UpdateOps.Type]])
    extends BaseFieldsParser[Map[FPath, UpdateOps.Type]] {
  def on(pathPrefix: FPath): UpdateFieldsParser[T] =
    UpdateFieldsParser[T](formatName, parsers.map {
      case (path, parser) ⇒ (pathPrefix / path) → parser
    })

  def on(pathPrefix: String): UpdateFieldsParser[T] = on(FPath(pathPrefix))

  def sequence: UpdateFieldsParser[T] =
    UpdateFieldsParser[T](s"seq($formatName)", parsers.map {
      case (path, parser) ⇒ (FPath.seq / path) → parser
    })

  def +(parser: (FPath, FieldsParser[UpdateOps.Type])): UpdateFieldsParser[T] =
    UpdateFieldsParser(formatName, parsers + parser)

  def ++(updateFieldsParser: UpdateFieldsParser[_]): UpdateFieldsParser[T] =
    UpdateFieldsParser(s"$formatName&${updateFieldsParser.formatName}", parsers ++ updateFieldsParser.parsers)

  private def parse(path: FPath, field: Field): Or[UpdateOps.Type, Every[AttributeError]] =
    parsers
      .get(path.toBare)
      .map(_.apply(path, field))
      .getOrElse(Bad(One(UnknownAttributeError(path.toString, field))))
  def apply(field: Field): Map[FPath, UpdateOps.Type] Or Every[AttributeError] =
    field match {
      case FObject(fields) ⇒
        fields.foldLeft[Map[FPath, UpdateOps.Type] Or Every[AttributeError]](Good(Map.empty)) {
          case (ops, (path, f)) ⇒
            withGood(ops, parse(FPath(path), f))((m, v) ⇒ m + (FPath(path) → v))
        }
      case _ ⇒ Good(Map.empty)
    }
}

object UpdateFieldsParser {
  def empty[T]: UpdateFieldsParser[T] =
    new UpdateFieldsParser[T]("empty", Map.empty)
  def apply[T](formatName: String)(parsers: (FPath, FieldsParser[UpdateOps.Type])*): UpdateFieldsParser[T] =
    UpdateFieldsParser[T](formatName, parsers.toMap)
  def apply[T]: UpdateFieldsParser[T] =
    macro FieldsParserMacro.getOrBuildUpdateFieldsParser[T]
}

class FieldsParser[T](val formatName: String, val acceptedInput: Seq[String], val parse: PartialFunction[(FPath, Field), T Or Every[AttributeError]])
    extends BaseFieldsParser[T] {

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
      case (path, field) ⇒ apply(path, field.get(pathElement))
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

  def sequence: FieldsParser[Seq[T]] =
    new FieldsParser[Seq[T]](
      s"seq($formatName)",
      acceptedInput.map(i ⇒ s"[$i]"), {
        case (path, field) ⇒
          field match {
            case FSeq(subFields) ⇒
              subFields.zipWithIndex
                .validatedBy { case (f, i) ⇒ apply(path.toSeq(i), f) }
            case FNull | FUndefined ⇒ Good(Nil)
            case other ⇒
              Bad(One(InvalidFormatAttributeError(path.toString, "object", Seq(s"[$formatName]"), other)))
          }
      }
    )

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

  def toUpdate: FieldsParser[UpdateOps.Type] = map(formatName) {
    case value: Option[_] ⇒
      value.fold[UpdateOps.Type](UpdateOps.UnsetAttribute)(v ⇒ UpdateOps.SetAttribute(v))
    case value ⇒ UpdateOps.SetAttribute(value)
  }
}

object FieldsParser {
  def apply[T](formatName: String, acceptedInput: Seq[String])(parse: PartialFunction[(FPath, Field), T Or Every[AttributeError]]) =
    new FieldsParser[T](formatName, acceptedInput, parse)
  def apply[T](formatName: String)(parse: PartialFunction[(FPath, Field), T Or Every[AttributeError]]) =
    new FieldsParser[T](formatName, Seq(formatName), parse)
  def good[T](value: T): FieldsParser[T] =
    new FieldsParser[T]("good", Nil, {
      case _ ⇒ Good(value)
    })
  def empty[T]: FieldsParser[T] = new FieldsParser[T]("empty", Nil, PartialFunction.empty)
  def apply[T]: FieldsParser[T] = macro FieldsParserMacro.getOrBuildFieldsParser[T]

  private def unlift[T, R](f: T ⇒ Option[R]): PartialFunction[T, R] =
    new PartialFunction[T, R] {
      def apply(x: T): R               = f(x).get
      def isDefinedAt(x: T): Boolean   = f(x).isDefined
      override def lift: T ⇒ Option[R] = f
    }

  def attachment: FieldsParser[Attachment] =
    FieldsParser[Attachment]("attachment") {
      case (_, f: FFile) ⇒ Good(f)
    }
  implicit val string: FieldsParser[String] = FieldsParser[String]("string") {
    case (_, FString(value)) ⇒ Good(value)
    case (_, FAny(s :: Nil)) ⇒ Good(s)
  }
  implicit val int: FieldsParser[Int] = FieldsParser[Int]("int")(unlift {
    case (_, FNumber(n))     ⇒ Some(Good(n.toInt))
    case (_, FAny(s :: Nil)) ⇒ Try(Good(s.toInt)).toOption
    case _                   ⇒ None
  })
  implicit val long: FieldsParser[Long] = FieldsParser[Long]("long")(unlift {
    case (_, FNumber(n))     ⇒ Some(Good(n.toLong))
    case (_, FAny(s :: Nil)) ⇒ Try(Good(s.toLong)).toOption
    case _                   ⇒ None
  })
  implicit val boolean: FieldsParser[Boolean] =
    FieldsParser[Boolean]("boolean")(unlift {
      case (_, FBoolean(b))    ⇒ Some(Good(b))
      case (_, FAny(s :: Nil)) ⇒ Try(Good(s.toBoolean)).toOption
      case _                   ⇒ None
    })
  implicit val date: FieldsParser[Date] = FieldsParser[Date]("date")(unlift {
    case (_, FNumber(n))     ⇒ Some(Good(new Date(n)))
    case (_, FAny(s :: Nil)) ⇒ Try(Good(new Date(s.toLong))).toOption
    case _                   ⇒ None
  })

  implicit val float: FieldsParser[Float] = FieldsParser[Float]("float")(unlift {
    case (_, FNumber(n))     ⇒ Some(Good(n.toFloat))
    case (_, FAny(s :: Nil)) ⇒ Try(Good(s.toFloat)).toOption
    case _                   ⇒ None
  })

  implicit val double: FieldsParser[Double] = FieldsParser[Double]("float")(unlift {
    case (_, FNumber(n))     ⇒ Some(Good(n.toDouble))
    case (_, FAny(s :: Nil)) ⇒ Try(Good(s.toDouble)).toOption
    case _                   ⇒ None
  })
}

object UpdateOps {
  sealed trait Type
  sealed trait DBType
  case class SetAttribute(value: Any) extends Type
  object UnsetAttribute               extends Type with DBType
}
