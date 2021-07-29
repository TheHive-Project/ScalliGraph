package org.thp.scalligraph.janus.strategies;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.query.JanusGraphPredicateUtils;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.tinkerpop.ElementUtils;
import org.janusgraph.graphdb.tinkerpop.optimize.HasStepFolder;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphStep;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.javatuples.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

// from https://raw.githubusercontent.com/JanusGraph/janusgraph/v0.5.3/janusgraph-core/src/main/java/org/janusgraph/graphdb/tinkerpop/optimize/JanusGraphStepStrategy.java
public class JanusGraphAcceptNullStrategy extends AbstractTraversalStrategy<ProviderOptimizationStrategy> implements ProviderOptimizationStrategy {
    private static final JanusGraphAcceptNullStrategy INSTANCE = new JanusGraphAcceptNullStrategy();

    private JanusGraphAcceptNullStrategy() {
    }

    public void apply(Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.onGraphComputer(traversal)) {
            TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach((originalGraphStep) -> {
                if (originalGraphStep.getIds() != null && originalGraphStep.getIds().length != 0) {
                    Object[] ids = originalGraphStep.getIds();
                    ElementUtils.verifyArgsMustBeEitherIdOrElement(ids);
                    if (ids[0] instanceof Element) {
                        Object[] elementIds = new Object[ids.length];

                        for (int i = 0; i < ids.length; ++i) {
                            elementIds[i] = ((Element) ids[i]).id();
                        }

                        originalGraphStep.setIteratorSupplier(() -> {
                            return originalGraphStep.returnsVertex() ? ((Graph) originalGraphStep.getTraversal().getGraph().get()).vertices(elementIds) : ((Graph) originalGraphStep.getTraversal().getGraph().get()).edges(elementIds);
                        });
                    }
                } else {
                    JanusGraphStepAcceptNull<?, ?> janusGraphStep = new JanusGraphStepAcceptNull(originalGraphStep);
                    TraversalHelper.replaceStep(originalGraphStep, janusGraphStep, traversal);
                    HasStepFolder.foldInIds(janusGraphStep, traversal);
                    foldInHasContainer(janusGraphStep, traversal, traversal);
                    foldInOrder(janusGraphStep, janusGraphStep.getNextStep(), traversal, traversal, janusGraphStep.returnsVertex(), (List) null);
                    foldInRange(janusGraphStep, JanusGraphTraversalUtil.getNextNonIdentityStep(janusGraphStep), traversal, (List) null);
                }

            });
        }
    }

    public static JanusGraphAcceptNullStrategy instance() {
        return INSTANCE;
    }

    private void foldInHasContainer(final HasStepFolder janusgraphStep, final Traversal.Admin<?, ?> traversal,
                                    final Traversal<?, ?> rootTraversal) {
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        while (true) {
            if (currentStep instanceof OrStep && (janusgraphStep instanceof JanusGraphStep || janusgraphStep instanceof  JanusGraphStepAcceptNull)) {
                for (final Traversal.Admin<?, ?> child : ((OrStep<?>) currentStep).getLocalChildren()) {
                    if (!HasStepFolder.validFoldInHasContainer(child.getStartStep(), false)){
                        return;
                    }
                }
                ((OrStep<?>) currentStep).getLocalChildren().forEach(t ->HasStepFolder.localFoldInHasContainer(janusgraphStep, t.getStartStep(), t, rootTraversal));
                currentStep.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(currentStep);
            } else if (currentStep instanceof HasContainerHolder){
                final Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers().stream().map(c -> JanusGraphPredicateUtils.convert(c)).collect(Collectors.toList());
                if  (HasStepFolder.validFoldInHasContainer(currentStep, true)) {
                    janusgraphStep.addAll(containers);
                    currentStep.getLabels().forEach(janusgraphStep::addLabel);
                    traversal.removeStep(currentStep);
                }
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep) && !(currentStep instanceof HasContainerHolder)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    private void foldInRange(final HasStepFolder janusgraphStep, final Step<?, ?>  tinkerpopStep, final Traversal.Admin<?, ?> traversal, final List<HasContainer> hasContainers) {
        final Step<?, ?> nextStep = tinkerpopStep instanceof IdentityStep ? JanusGraphTraversalUtil.getNextNonIdentityStep(tinkerpopStep): tinkerpopStep;
        if (nextStep instanceof RangeGlobalStep) {
            final RangeGlobalStep range = (RangeGlobalStep) nextStep;
            int low = 0;
            if (janusgraphStep instanceof JanusGraphStep || janusgraphStep instanceof JanusGraphStepAcceptNull) {
                low = QueryUtil.convertLimit(range.getLowRange());
                low = QueryUtil.mergeLowLimits(low, hasContainers == null ? janusgraphStep.getLowLimit(): janusgraphStep.getLocalLowLimit(hasContainers));
            }
            int high = QueryUtil.convertLimit(range.getHighRange());
            high = QueryUtil.mergeHighLimits(high, hasContainers == null ? janusgraphStep.getHighLimit(): janusgraphStep.getLocalHighLimit(hasContainers));
            if (hasContainers == null) {
                janusgraphStep.setLimit(low, high);
            } else {
                janusgraphStep.setLocalLimit(hasContainers, low, high);
            }
            if (janusgraphStep instanceof JanusGraphStep || janusgraphStep instanceof JanusGraphStepAcceptNull || range.getLowRange() == 0) { //Range can be removed since there is JanusGraphStep or no offset
                nextStep.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(nextStep);
            }
        }
    }

    private Step<?, ?> foldInOrder(final HasStepFolder janusgraphStep, final Step<?, ?> tinkerpopStep, final Traversal.Admin<?, ?> traversal,
                                   final Traversal<?, ?> rootTraversal, boolean isVertexOrder, final List<HasContainer> hasContainers) {
        Step<?, ?> currentStep = tinkerpopStep;
        OrderGlobalStepAcceptNull<?, ?> lastOrder = null;
        while (true) {
            if (currentStep instanceof OrderGlobalStepAcceptNull) {
                if (lastOrder != null) { //Previous orders are rendered irrelevant by next order (since re-ordered)
                    lastOrder.getLabels().forEach(janusgraphStep::addLabel);
                    traversal.removeStep(lastOrder);
                }
                lastOrder = (OrderGlobalStepAcceptNull) currentStep;
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof HasStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }

        if (lastOrder != null && validJanusGraphOrder(lastOrder, rootTraversal, isVertexOrder)) {
            //Add orders to HasStepFolder
            for (final Pair<Traversal.Admin<Object, Comparable>, Comparator<Object>> comp : (List<Pair<Traversal.Admin<Object, Comparable>, Comparator<Object>>>) ((OrderGlobalStepAcceptNull) lastOrder).getComparators()) {
                final String key;
                final Order order;
                if (comp.getValue0() instanceof ElementValueTraversal) {
                    final ElementValueTraversal evt = (ElementValueTraversal) comp.getValue0();
                    key = evt.getPropertyKey();
                    order = (Order) comp.getValue1();
                } else {
                    final ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
                    key = evc.getPropertyKey();
                    order = (Order) evc.getValueComparator();
                }
                if (hasContainers == null) {
                    janusgraphStep.orderBy(key, order);
                } else {
                    janusgraphStep.localOrderBy(hasContainers, key, order);
                }
            }
            lastOrder.getLabels().forEach(janusgraphStep::addLabel);
            traversal.removeStep(lastOrder);
        }
        return currentStep;
    }

    private boolean validJanusGraphOrder(OrderGlobalStepAcceptNull orderGlobalStep, Traversal rootTraversal,
                                         boolean isVertexOrder) {
        final List<Pair<Traversal.Admin, Object>> comparators = orderGlobalStep.getComparators();
        for (final Pair<Traversal.Admin, Object> comp : comparators) {
            final String key;
            if (comp.getValue0() instanceof ElementValueTraversal &&
                    comp.getValue1() instanceof Order) {
                key = ((ElementValueTraversal) comp.getValue0()).getPropertyKey();
            } else if (comp.getValue1() instanceof ElementValueComparator) {
                final ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
                if (!(evc.getValueComparator() instanceof Order)) return false;
                key = evc.getPropertyKey();
            } else {
                // do not fold comparators that include nested traversals that are not simple ElementValues
                return false;
            }
            final JanusGraphTransaction tx = JanusGraphTraversalUtil.getTx(rootTraversal.asAdmin());
            final PropertyKey pKey = tx.getPropertyKey(key);
            if (pKey == null
                    || !(Comparable.class.isAssignableFrom(pKey.dataType()))
                    || (isVertexOrder && pKey.cardinality() != Cardinality.SINGLE)) {
                return false;
            }
        }
        return true;
    }
}
