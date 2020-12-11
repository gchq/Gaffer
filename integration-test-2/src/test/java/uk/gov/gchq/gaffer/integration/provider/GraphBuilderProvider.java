package uk.gov.gchq.gaffer.integration.provider;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.integration.util.TestUtil.createDefaultSchema;

public class GraphBuilderProvider extends StorePropertiesProvider {

    private final List<Graph.Builder> builders;

    public GraphBuilderProvider() {
        super();
        final Schema schema = createDefaultSchema();

        builders = getStoreProperties().stream().map(sp -> new Graph.Builder()
                .addSchema(schema)
                .config(new GraphConfig("test"))
                .storeProperties(sp)).collect(Collectors.toList());
    }

    protected List<Graph.Builder> getBuilders() {
        return builders;
    }



    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
        TraitRequirement requirement = extractTraitRequirement(extensionContext);
        return builders.stream().filter(builder -> builder.build().getStoreTraits().containsAll(Arrays.asList(requirement.value()))).map(Arguments::of);
    }
}
