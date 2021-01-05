package org.thp.scalligraph.traversal

import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.{HasStep, NotStep, TraversalFilterStep}
import org.apache.tinkerpop.gremlin.process.traversal.step.map.{GraphStep, OrderGlobalStep}
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.{FilterRankingStrategy, InlineFilterStrategy}
import org.apache.tinkerpop.gremlin.process.traversal.{Step, TraversalStrategy}
import play.api.Logger

import java.util.{Arrays => JArrays, HashSet => JHashSet, List => JList, Set => JSet}

object OptimizeIndexStrategy extends AbstractTraversalStrategy[TraversalStrategy.OptimizationStrategy] with TraversalStrategy.OptimizationStrategy {

  lazy val logger: Logger = Logger(getClass)

  private def apply(traversal: Admin[_, _], steps: JList[Step[_, _]], i: Int): Int =
    if (i >= steps.size() - 1) i
    else {
      val step = steps.get(i)
      step match {
        case _: HasStep[_] | _: OrderGlobalStep[_, _] => apply(traversal, steps, i + 1)
        case _: TraversalFilterStep[_] | _: NotStep[_] =>
          traversal.removeStep(i)
          val newPos = apply(traversal, traversal.getSteps, i)
          traversal.addStep(newPos, step)
          newPos
        case _ => i
      }
    }

  override def apply(traversal: Admin[_, _]): Unit =
    if (traversal.getStartStep.isInstanceOf[GraphStep[_, _]])
      if (logger.isDebugEnabled) {
        val in = traversal.toString
        apply(traversal, traversal.getSteps, 1)
        logger.trace(s"Run optimisation\nin:  $in\nout: $traversal")
      } else apply(traversal, traversal.getSteps, 1)

  override lazy val applyPrior: JSet[Class[_ <: TraversalStrategy.OptimizationStrategy]] =
    new JHashSet(JArrays.asList(classOf[InlineFilterStrategy], classOf[FilterRankingStrategy]))
}
