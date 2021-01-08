package uk.gov.gchq.gaffer.integration.extensions;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * An AbstractContextProvider provides context for a TestCase class. It will be used when a test method takes an
 * argument of type T as a parameter.
 *
 * The AbstractContextProvider works by using the store properties on the classpath of the test which is using it.
 * The function for transforming the store properties into a testcase is done on a per implementation basis.
 * @param <T> The type of TestCase
 */
public abstract class AbstractContextProvider<T extends AbstractTestCase> implements TestTemplateInvocationContextProvider {

        private static final String TEMPDIR = "${tempdir}";

        protected abstract Class<T> getTestCaseClass();

        protected abstract Stream<T> toTestCases(final StoreProperties storeProperties);

        @Override
        public boolean supportsTestTemplate(final ExtensionContext context) {
            for (final Class<?> parameterType : context.getRequiredTestMethod().getParameterTypes()) {
                if (parameterType.equals(getTestCaseClass())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(final ExtensionContext context) {
            return
                Arrays.stream(StreamUtil.openStreams(context.getRequiredTestClass(), "/stores"))
                .map(StoreProperties::loadStoreProperties)
                .map(this::injectTempDir)
                .filter(sp -> this.isNotExcluded(sp, context))
                .flatMap(this::toTestCases)
                .filter(tc -> this.hasRequiredTraits(tc, context))
                .map(this::createContext);
        }

        private boolean isNotExcluded(final StoreProperties storeProperties, final ExtensionContext context) {
            GafferTest classAnnotation = context.getRequiredTestClass().getDeclaredAnnotation(GafferTest.class);
            List<Class<? extends Store>> excludedStores = classAnnotation == null ? new ArrayList<>() : Arrays.asList(classAnnotation.excludeStores());
            excludedStores.addAll(Lists.newArrayList(context.getRequiredTestMethod().getDeclaredAnnotation(GafferTest.class).excludeStores()));
            for (final Class<? extends Store> storeClass : excludedStores) {
                try {
                    if (storeClass.isAssignableFrom(Class.forName(storeProperties.getStoreClass()))) {
                        return false;
                    }
                } catch (final Exception e) {
                    throw new RuntimeException("Failed to resolve the store class in the store properties");
                }
            }
            return true;
        }

        private StoreProperties injectTempDir(final StoreProperties storeProperties) {
            Map<String, String> replacements = new HashMap<>();
            storeProperties.getProperties().forEach((k, v) -> {
                if (v instanceof String && k instanceof String && ((String) v).contains(TEMPDIR)) {
                    String replacement = ((String) v).replace(TEMPDIR, AbstractStoreIT.tempDir.toString());
                    replacements.put((String) k, replacement);
                }
            });

            replacements.forEach(storeProperties::set);

            return storeProperties;
        }

        private boolean hasRequiredTraits(final T testCase, final ExtensionContext context) {
            TraitRequirement requirement = context.getTestMethod().isPresent() ? context.getTestMethod().get()
                .getDeclaredAnnotation(TraitRequirement.class) : null;

            try {
                return requirement == null || testCase.getGraph().getStoreTraits().containsAll(Arrays.asList(requirement.value()));
            } catch (final Exception e) {
                throw new RuntimeException("Failed to create store", e);
            }
        }

        private TestTemplateInvocationContext createContext(final T testCase) {
            return new TestTemplateInvocationContext() {
                @Override
                public String getDisplayName(final int invocationIndex) {
                    return "[" + invocationIndex + "] " + testCase.getTestName();
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
