package org.thp.scalligraph

import java.lang.{Boolean ⇒ JBoolean, Byte ⇒ JByte, Double ⇒ JDouble, Float ⇒ JFloat, Long ⇒ JLong, Short ⇒ JShort}

object Utils {

  def convertToJava(c: Class[_]): Class[_] = c match {
    case JByte.TYPE     ⇒ classOf[JByte]
    case JShort.TYPE    ⇒ classOf[Short]
    case Character.TYPE ⇒ classOf[Character]
    case Integer.TYPE   ⇒ classOf[Integer]
    case JLong.TYPE     ⇒ classOf[JLong]
    case JFloat.TYPE    ⇒ classOf[JFloat]
    case JDouble.TYPE   ⇒ classOf[JDouble]
    case JBoolean.TYPE  ⇒ classOf[JBoolean]
    case _              ⇒ c
  }
}
