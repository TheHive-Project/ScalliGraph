package org.thp.scalligraph.controllers

import scala.language.experimental.macros

import org.scalactic.Accumulation._
import org.scalactic.{Every, Or}
import org.thp.scalligraph.macros.FieldsParserMacro
import org.thp.scalligraph.{AttributeError, FPath, FPathElem, FPathSeq}

case class UpdateFieldsParser[T](formatName: String, parsers: Seq[(FPath, FieldsParser[_])]) {

  def ++(updateFieldsParser: UpdateFieldsParser[_]): UpdateFieldsParser[T] =
    new UpdateFieldsParser[T](formatName, parsers ++ updateFieldsParser.parsers)

  def forType[A](newFormatName: String) = new UpdateFieldsParser[A](newFormatName, parsers)

  def apply(field: FObject): Seq[(FPath, Any)] Or Every[AttributeError] = {
    println(s"parsing $formatName \n$field\n with ${parsers.map(p ⇒ "\n - " + p.toString)}")
    field.fields.toSeq.flatMap {
      case (key, value) ⇒
        val path = FPath(key)
        parsers.collectFirst {
          case (p, parser) if p.matches(path) ⇒
            parser(value)
              .map(path → _)
              .badMap(x ⇒ x.map(_.withName(path.toString)))
        }
    }.combined
  }

  def on(pathStr: String): UpdateFieldsParser[T] =
    new UpdateFieldsParser[T](formatName, parsers.map { case (path, parser) ⇒ FPathElem(pathStr, path) → parser })

  def seq(pathStr: String): UpdateFieldsParser[T] = {
    println(s"[$formatName] seq($pathStr) => ${parsers.map(_._1.toString)}")
    new UpdateFieldsParser[T](formatName, parsers.map { case (path, parser) ⇒ FPathSeq(pathStr, path) → parser })
  }
}

object UpdateFieldsParser {
  def empty[T](formatName: String): UpdateFieldsParser[T] = new UpdateFieldsParser[T](formatName, Nil)
  def apply[T]: UpdateFieldsParser[T] = macro FieldsParserMacro.getOrBuildUpdateFieldsParser[T]
}
