package org.thp.scalligraph.macros

import org.thp.scalligraph.MacroUtil

import scala.reflect.macros.whitebox

class EntityMacro(val c: whitebox.Context) extends MacroUtil {
  import c.universe._

  def copyEntity[T: WeakTypeTag](to: Tree): Tree = {
    val entity = c.macroApplication
    val tType  = weakTypeOf[T]
    tType match {
      case CaseClassType(symbols @ _*) ⇒
        val fields = symbols.map(s ⇒ q"$to.$s")
        q"""
          new $tType(..$fields) with org.thp.scalligraph.models.Entity {
            val _id = $entity._id
            val _model = $entity._model
            val _createdBy = $entity._createdBy
            val _updatedBy = $entity._updatedBy
            val _createdAt = $entity._createdAt
            val _updatedAt = $entity._createdBy
          }
        """

    }
  }

}
