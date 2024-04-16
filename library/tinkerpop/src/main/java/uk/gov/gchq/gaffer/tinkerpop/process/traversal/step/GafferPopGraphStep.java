package uk.gov.gchq.gaffer.tinkerpop.process.traversal.step;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

public class GafferPopGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopGraphStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final GafferPopGraph graph;
    private Map<String, Object> options = new HashMap<>();

    public GafferPopGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        LOGGER.info("ORIGN LABELS: {}", getLabels());

        // Save reference to the graph
        this.graph = (GafferPopGraph) originalGraphStep.getTraversal().getGraph().get();
        LOGGER.info("Running custom step on Graph: {}", graph.configuration().getString(GafferPopGraph.GRAPH_ID));

        // Find any options on the traversal
        Optional<OptionsStrategy> optionsStrategy = originalGraphStep.getTraversal().getStrategies().getStrategy(OptionsStrategy.class);
        if (optionsStrategy.isPresent()) {
            LOGGER.info("OPTIONS PRESENT");
            options = optionsStrategy.get().getOptions();
            options.forEach((k, v) -> LOGGER.info("KEy: " + k + "Val: " + v));
        }

        // Set the output iterator to the relevant filtered output from the class methods
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        // Check for and labels being searched for to construct a View to filter with
        List<String> labels = getRequestedLabels();

        if (!labels.isEmpty()) {
            LOGGER.info("FOUND LABELS {}", labels);
            // Find using label to filter results
            return graph.edges(Arrays.asList(this.ids), labels.toArray(new String[0]));
        }

        // linear scan as fallback
        return IteratorUtils.filter(graph.edges(Arrays.asList(this.ids)), edge -> HasContainer.testAll(edge, hasContainers));
    }

    private Iterator<? extends Vertex> vertices() {
        LOGGER.info("IDS ARE {}", this.ids);

        // Check for and labels being searched for to construct a View to filter with
        List<String> labels = getRequestedLabels();

        if (!labels.isEmpty()) {
            LOGGER.info("FOUND LABELS {}", labels);
            // Find using label to filter results
            return graph.vertices(Arrays.asList(this.ids), labels.toArray(new String[0]));
        }

        // linear scan as fallback
        LOGGER.info("Liner scan fallback");
        return IteratorUtils.filter(graph.vertices(Arrays.asList(this.ids)), vertex -> HasContainer.testAll(vertex, hasContainers));
    }

    private List<String> getRequestedLabels() {
        return hasContainers.stream()
            .filter(hc -> hc.getKey() != null && hc.getKey().equals(T.label.getAccessor()))
            .filter(hc -> Compare.eq == hc.getBiPredicate())
            .filter(hc -> hc.getValue() != null)
            .map(hc -> (String) hc.getValue())
            .collect(Collectors.toList());
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            ((AndP<?>) hasContainer.getPredicate()).getPredicates().forEach(
                p -> this.addHasContainer(new HasContainer(hasContainer.getKey(), p)));
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }

}
