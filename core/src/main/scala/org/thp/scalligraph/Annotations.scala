package org.thp.scalligraph

import org.thp.scalligraph.macros.AnnotationMacro

import scala.annotation.{compileTimeOnly, StaticAnnotation}
import scala.language.experimental.{macros => enableMacro}

@compileTimeOnly("enable macro paradise to expand macro annotations")
class BuildVertexEntity extends StaticAnnotation {

  def macroTransform(annottees: Any*): Any =
    macro AnnotationMacro.buildVertexModel
}

@compileTimeOnly("enable macro paradise to expand macro annotations")
class BuildEdgeEntity[FROM <: Product, TO <: Product] extends StaticAnnotation {

  def macroTransform(annottees: Any*): Any =
    macro AnnotationMacro.buildEdgeModel
}

@compileTimeOnly("enable macro paradise to expand macro annotations")
class EntitySteps[E <: Product] extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro AnnotationMacro.entitySteps
}
