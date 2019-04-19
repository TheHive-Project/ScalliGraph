package org.thp.scalligraph.controllers

import scala.language.experimental.macros

import org.thp.scalligraph.macros.FieldsParserMacro

trait TestUtils {
  def getFieldsParser[T]: FieldsParser[T] = macro FieldsParserMacro.getOrBuildFieldsParser[T]
  def getUpdateFieldsParser[T]: UpdateFieldsParser[T] = macro FieldsParserMacro.getOrBuildUpdateFieldsParser[T]
}
