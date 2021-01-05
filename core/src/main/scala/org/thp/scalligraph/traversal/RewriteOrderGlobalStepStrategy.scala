package org.thp.scalligraph.traversal

import org.apache.tinkerpop.gremlin.process.traversal.{Order, Step, TraversalStrategy, Traverser}
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin
import org.apache.tinkerpop.gremlin.process.traversal.lambda.{AbstractLambdaTraversal, ElementValueTraversal}
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep
import org.apache.tinkerpop.gremlin.process.traversal.util.{TraversalHelper, TraversalUtil}
import org.apache.tinkerpop.gremlin.structure.Element

import java.util.Comparator
import scala.collection.JavaConverters._

object RewriteOrderGlobalStepStrategy extends TraversalStrategy.OptimizationStrategy {
  override def apply(traversal: Admin[_, _]): Unit = {
    val steps = TraversalHelper.getStepsOfAssignableClass(classOf[OrderGlobalStep[_, _]], traversal)
    steps.asScala.filterNot(_.getComparators.isEmpty).foreach { step =>
      val newStep = new OrderGlobalStep[Element, Comparable[AnyRef]](traversal)
      step.getComparators.asScala.map(p => (p.getValue0, p.getValue1)).foreach {
        case (evt: ElementValueTraversal[_], order: Order) =>
          newStep.addComparator(ElementValueTraversalDummy[Comparable[AnyRef]](evt.getPropertyKey), ComparatorWrapper(order))
        case (t, order: Order) =>
          newStep.addComparator(t.asInstanceOf[Admin[Element, Comparable[AnyRef]]], ComparatorWrapper(order))
        case (t, c) =>
          newStep.addComparator(t.asInstanceOf[Admin[Element, Comparable[AnyRef]]], c.asInstanceOf[Comparator[Comparable[AnyRef]]])
      }
      step.getLabels.forEach(newStep.addLabel(_))
      TraversalHelper.replaceStep(step.asInstanceOf[Step[Element, Element]], newStep, traversal)
    }
  }
}
case class ElementValueTraversalDummy[T](propertyKey: String) extends AbstractLambdaTraversal[Element, T] {
  private var value: T          = _
  override def next(): T        = value
  override def hasNext: Boolean = true
  override def addStart(start: Traverser.Admin[Element]): Unit =
    if (bypassTraversal == null) value = start.get.property[T](propertyKey).orElse(null.asInstanceOf[T])
    else value = TraversalUtil.apply(start, bypassTraversal)
}

case class ComparatorWrapper(comparator: Comparator[AnyRef]) extends Comparator[Comparable[AnyRef]] {
  override def compare(a: Comparable[AnyRef], b: Comparable[AnyRef]): Int =
    if (a == null && b == null) 0
    else if (a == null) 1
    else if (b == null) -1
    else comparator.asInstanceOf[Comparator[Comparable[AnyRef]]].compare(a, b)
}
