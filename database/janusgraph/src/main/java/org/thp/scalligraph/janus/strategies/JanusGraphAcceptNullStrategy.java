package org.thp.scalligraph.janus.strategies;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.graphdb.tinkerpop.ElementUtils;
import org.janusgraph.graphdb.tinkerpop.optimize.HasStepFolder;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;

// from https://raw.githubusercontent.com/JanusGraph/janusgraph/v0.5.3/janusgraph-core/src/main/java/org/janusgraph/graphdb/tinkerpop/optimize/JanusGraphStepStrategy.java
public class JanusGraphAcceptNullStrategy extends AbstractTraversalStrategy<ProviderOptimizationStrategy> implements ProviderOptimizationStrategy {
    private static final JanusGraphAcceptNullStrategy INSTANCE = new JanusGraphAcceptNullStrategy();

    private JanusGraphAcceptNullStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            if (originalGraphStep.getIds() == null || originalGraphStep.getIds().length == 0) {
                //Try to optimize for index calls
                final JanusGraphStepAcceptNull<?, ?> janusGraphStep = new JanusGraphStepAcceptNull<>(originalGraphStep);
                TraversalHelper.replaceStep(originalGraphStep, janusGraphStep, traversal);
                HasStepFolder.foldInIds(janusGraphStep, traversal);
                HasStepFolder.foldInHasContainer(janusGraphStep, traversal, traversal);
                HasStepFolder.foldInOrder(janusGraphStep, janusGraphStep.getNextStep(), traversal, traversal, janusGraphStep.returnsVertex(), null);
                HasStepFolder.foldInRange(janusGraphStep, JanusGraphTraversalUtil.getNextNonIdentityStep(janusGraphStep), traversal, null);
            } else {
                //Make sure that any provided "start" elements are instantiated in the current transaction
                final Object[] ids = originalGraphStep.getIds();
                ElementUtils.verifyArgsMustBeEitherIdOrElement(ids);
                if (ids[0] instanceof Element) {
                    //GraphStep constructor ensures that the entire array is elements
                    final Object[] elementIds = new Object[ids.length];
                    for (int i = 0; i < ids.length; i++) {
                        elementIds[i] = ((Element) ids[i]).id();
                    }
                    originalGraphStep.setIteratorSupplier(() -> originalGraphStep.returnsVertex() ?
                            ((Graph) originalGraphStep.getTraversal().getGraph().get()).vertices(elementIds) :
                            ((Graph) originalGraphStep.getTraversal().getGraph().get()).edges(elementIds));
                }
            }

        });
    }

    public static JanusGraphAcceptNullStrategy instance() {
        return INSTANCE;
    }
}
