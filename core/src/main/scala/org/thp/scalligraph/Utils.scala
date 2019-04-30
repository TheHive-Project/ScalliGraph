package org.thp.scalligraph

object Utils {
  def convertToJava(c: Class[_]): Class[_] = c match {
    case java.lang.Byte.TYPE      ⇒ classOf[java.lang.Byte]
    case java.lang.Short.TYPE     ⇒ classOf[java.lang.Short]
    case java.lang.Character.TYPE ⇒ classOf[java.lang.Character]
    case java.lang.Integer.TYPE   ⇒ classOf[java.lang.Integer]
    case java.lang.Long.TYPE      ⇒ classOf[java.lang.Long]
    case java.lang.Float.TYPE     ⇒ classOf[java.lang.Float]
    case java.lang.Double.TYPE    ⇒ classOf[java.lang.Double]
    case java.lang.Boolean.TYPE   ⇒ classOf[java.lang.Boolean]
    case _                        ⇒ c
  }
}
