/*
 * Copyright 2020 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Provides the test case for a given test. It uses the store properties files located at /stores/ on the classpath.
 * For each test, if the TraitRequirement annotation is set, this extensions will only provide stores which have that
 * meet the TraitRequirement.
 */
public class GafferTestContextProvider implements TestTemplateInvocationContextProvider {

    private static final String TEMPDIR = "${tempdir}";
    private final List<StoreProperties> availableStoreProperties;

    public GafferTestContextProvider() {
        availableStoreProperties = new ArrayList<>();
        for (final InputStream storeInputStream : StreamUtil.openStreams(getClass(), "/stores")) {
            availableStoreProperties.add(StoreProperties.loadStoreProperties(storeInputStream));
        }
    }

    @Override
    public boolean supportsTestTemplate(final ExtensionContext context) {
        for (final Class<?> parameterType : context.getRequiredTestMethod().getParameterTypes()) {
            if (GafferTestCase.class.equals(parameterType)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(final ExtensionContext context) {
        return availableStoreProperties.stream()
                .map(this::injectTempDir)
                .filter(sp -> this.isNotExcluded(sp, context))
                .map(GafferTestCase::new)
                .filter(gtc -> this.hasRequiredTraits(gtc, context))
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

    private boolean hasRequiredTraits(final GafferTestCase testCase, final ExtensionContext context) {
        TraitRequirement requirement = context.getTestMethod().isPresent() ? context.getTestMethod().get()
                .getDeclaredAnnotation(TraitRequirement.class) : null;

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
