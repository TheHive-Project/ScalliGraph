package org.thp.scalligraph.traversal

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource

import scala.reflect.ClassTag

trait GraphStrategy extends (GraphTraversalSource => GraphTraversalSource)

case class CustomClassStrategy(
    withStrategies: Seq[TraversalStrategy[_ <: TraversalStrategy[_]]],
    withoutStrategies: Seq[Class[TraversalStrategy[_ <: TraversalStrategy[_]]]]
) extends GraphStrategy {
  def apply(source: GraphTraversalSource): GraphTraversalSource =
    source.withStrategies(withStrategies: _*).withoutStrategies(withoutStrategies: _*)
}

object GraphStrategy {
  def `with`(strategy: TraversalStrategy[_ <: TraversalStrategy[_]]): CustomClassStrategy = CustomClassStrategy(Seq(strategy), Nil)
//  def without(strategy: TraversalStrategy[_ <: TraversalStrategy[_]]): CustomClassStrategy = CustomClassStrategy(Nil, Seq(strategy.getClass))

  def without[S <: TraversalStrategy[_]](implicit classTag: ClassTag[S]): CustomClassStrategy =
    CustomClassStrategy(Nil, Seq(classTag.runtimeClass.asInstanceOf[Class[TraversalStrategy[_ <: TraversalStrategy[_]]]]))

  val default: GraphStrategy = Predef.identity[GraphTraversalSource]
}
