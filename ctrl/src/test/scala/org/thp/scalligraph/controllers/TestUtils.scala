package org.thp.scalligraph.controllers

import org.thp.scalligraph.macros.FieldsParserMacro

import scala.language.experimental.macros

trait TestUtils {
  def getFieldsParser[T]: FieldsParser[T] = macro FieldsParserMacro.getFieldsParser[T]
  def getUpdateFieldsParser[T]: UpdateFieldsParser[T] = macro FieldsParserMacro.getUpdateFieldsParser[T]
}
