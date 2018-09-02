package org.thp.scalligraph.macros

case class MacroError(message: String, cause: Option[Throwable] = None) extends Exception(message, cause.orNull)
