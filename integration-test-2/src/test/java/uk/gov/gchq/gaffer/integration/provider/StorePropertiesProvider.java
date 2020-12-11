package uk.gov.gchq.gaffer.integration.provider;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class StorePropertiesProvider implements ArgumentsProvider {

    private final List<StoreProperties> storeProperties = Lists.newArrayList(
            StoreProperties.loadStoreProperties(StreamUtil.openStream(EmptyGraphProvider.class, "/stores/mapstore.properties")),
            StoreProperties.loadStoreProperties(StreamUtil.openStream(EmptyGraphProvider.class, "/stores/accumulostore.properties"))
    );

    protected List<StoreProperties> getStoreProperties() {
        return storeProperties;
    }

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
        TraitRequirement requirement = extractTraitRequirement(extensionContext);
        return storeProperties.stream().filter(sp -> hasTraits(requirement, sp)).map(Arguments::of);
    }

    protected TraitRequirement extractTraitRequirement(final ExtensionContext extensionContext) {
        Optional<Method> testMethod = extensionContext.getTestMethod();
        return testMethod.isPresent() ? testMethod.get().getDeclaredAnnotation(TraitRequirement.class) : null;
    }

    private boolean hasTraits(final TraitRequirement requirement, final StoreProperties properties) {
        if (requirement == null) {
            return true;
        }
        try {
            Store store = (Store) Class.forName(properties.getStoreClass()).newInstance();
            return store.getTraits().containsAll(Arrays.asList(requirement.value()));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could find store class: " + properties.getStoreClass(), e);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IllegalArgumentException("Could not create store", e);
        }
    }
}
