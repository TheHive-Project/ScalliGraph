package org.thp.scalligraph

import scala.reflect.runtime.universe._

object RichType {
  def getTypeArgs(t: Type, fromType: Type): List[Type] =
    if (t.typeSymbol == fromType.typeSymbol)
      t.typeArgs
    else
      internal
        .thisType(t.dealias.typeSymbol)
        .baseType(fromType.typeSymbol.asClass)
        .typeArgs
}
