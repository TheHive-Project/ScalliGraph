package org.thp.scalligraph.janus.strategies;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.function.MultiComparator;
import org.janusgraph.graphdb.tinkerpop.optimize.step.HasStepFolder.OrderEntry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// From https://github.com/JanusGraph/janusgraph/blob/v0.6.0/janusgraph-core/src/main/java/org/janusgraph/graphdb/util/MultiDistinctOrderedIterator.java
// s/ElementValueComparator/ElementValueComparatorAcceptNull/g
public class MultiDistinctOrderedIteratorAcceptNull<E extends Element> implements CloseableIterator<E> {

    private final Map<Integer, Iterator<E>> iterators = new LinkedHashMap<>();
    private final Map<Integer, E> values = new LinkedHashMap<>();
    private final TreeMap<E, Integer> currentElements;
    private final Set<Object> allElements = new HashSet<>();
    private final Integer limit;
    private long count = 0;

    public MultiDistinctOrderedIteratorAcceptNull(final Integer lowLimit, final Integer highLimit, final List<Iterator<E>> iterators, final List<OrderEntry> orders) {
        this.limit = highLimit;
        final List<Comparator<E>> comp = new ArrayList<>();
        orders.forEach(o -> comp.add(new ElementValueComparatorAcceptNull(o.key, o.order)));
        Comparator<E> comparator = new MultiComparator<>(comp);
        for (int i = 0; i < iterators.size(); i++) {
            this.iterators.put(i, iterators.get(i));
        }
        currentElements = new TreeMap<>(comparator);
        long i = 0;
        while (i < lowLimit && this.hasNext()) {
            this.next();
            i++;
        }
    }

    @Override
    public boolean hasNext() {
        if (limit != null && count >= limit) {
            return false;
        }
        for (int i = 0; i < iterators.size(); i++) {
            if (!values.containsKey(i) && iterators.get(i).hasNext()){
                E element = null;
                do {
                    element = iterators.get(i).next();
                    if (allElements.contains(element.id())) {
                        element = null;
                    }
                } while (element == null && iterators.get(i).hasNext());
                if (element != null) {
                    values.put(i, element);
                    currentElements.put(element, i);
                    allElements.add(element.id());
                }
            }
        }
        return !values.isEmpty();
    }

    @Override
    public E next() {
        count++;
        return values.remove(currentElements.remove(currentElements.firstKey()));
    }

    @Override
    public void close() {
        iterators.values().forEach(CloseableIterator::closeIterator);
    }
}