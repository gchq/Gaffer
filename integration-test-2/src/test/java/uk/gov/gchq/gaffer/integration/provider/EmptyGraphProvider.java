package uk.gov.gchq.gaffer.integration.provider;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.TraitRequirement;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EmptyGraphProvider extends GraphBuilderProvider {

    private final List<Graph> graphs;

    public EmptyGraphProvider() {
        super();
        this.graphs = getBuilders().stream().map(Graph.Builder::build).collect(Collectors.toList());
    }

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
        TraitRequirement requirement = extractTraitRequirement(extensionContext);
        return graphs.stream()
                .filter(graph -> requirement == null || graph.getStoreTraits().containsAll(Arrays.asList(requirement.value())))
                .map(Arguments::of);
    }

    protected List<Graph> getGraphs() {
        return graphs;
    }


}
