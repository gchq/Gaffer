package uk.gov.gchq.gaffer.integration.extensions;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Provides the test case for a given test. It uses the store properties files located at /stores/ on the classpath.
 * For each test, if the TraitRequirement annotation is set, this extensions will only provide stores which have that
 * meet the TraitRequirement.
 */
public class GafferTestContextProvider implements TestTemplateInvocationContextProvider {

    private final List<StoreProperties> availableStoreProperties;

    public GafferTestContextProvider() {
        availableStoreProperties = new ArrayList<>();
        for (InputStream storeInputStream : StreamUtil.openStreams(AbstractStoreIT.class, "/stores")) {
            availableStoreProperties.add(StoreProperties.loadStoreProperties(storeInputStream));
        }
    }

    @Override
    public boolean supportsTestTemplate(final ExtensionContext context) {
        for (Class<?> parameterType : context.getRequiredTestMethod().getParameterTypes()) {
            if (GafferTestCase.class.equals(parameterType)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(final ExtensionContext context) {
        return availableStoreProperties.stream()
                .map(GafferTestCase::new)
                .filter(gtc -> this.hasRequiredTraits(gtc, context))
                .map(this::createContext);
    }

    private boolean hasRequiredTraits(final GafferTestCase testCase, final ExtensionContext context) {
        TraitRequirement requirement = context.getTestMethod().isPresent() ? context.getTestMethod().get()
                .getDeclaredAnnotation(TraitRequirement.class): null;

        try {
            return requirement == null || testCase.getEmptyGraph().getStoreTraits().containsAll(Arrays.asList(requirement.value()));
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create store", e);
        }
    }

    private TestTemplateInvocationContext createContext(final GafferTestCase testCase) {
        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(final int invocationIndex) {
                return testCase.getStoreType();
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return Lists.newArrayList(
                        new GenericTypedParameterResolver<>(testCase)
                );
            }
        };
    }
}
