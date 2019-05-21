package org.thp.scalligraph.query

import org.thp.scalligraph.controllers._

object FObjOne {

  def unapply(field: Field): Option[(String, Field)] = field match {
    case FObject(f) if f.size == 1 ⇒ Some(f.head)
    case _                         ⇒ None
  }
}

object FNamedObj {

  def unapply(field: Field): Option[(String, FObject)] = field match {
    case f: FObject ⇒
      f.get("_name") match {
        case FString(name) ⇒ Some(name → (f - "_name"))
        case _             ⇒ None
      }
    case _ ⇒ None
  }
}

object FNative {

  def unapply(field: Field): Option[Any] = field match {
    case FString(s)  ⇒ Some(s)
    case FNumber(n)  ⇒ Some(n)
    case FBoolean(b) ⇒ Some(b)
    case FAny(a)     ⇒ Some(a.mkString)
    case _           ⇒ None
  }
}
