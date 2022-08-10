/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.junit.extensions;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.platform.commons.support.ReflectionSupport.tryToLoadClass;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

public class IntegrationTestSuiteExtension implements ParameterResolver, BeforeAllCallback, BeforeEachCallback, ExecutionCondition {

    static {
        ExtensionContext.Namespace.create(IntegrationTestSuiteExtension.class);
    }

    public static final String INIT_CLASS = "initClass";

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestSuiteExtension.class);

    private static final Map<String, IntegrationTestSuite> INTEGRATION_TEST_SUITE_CLASS_MAP = new HashMap<>();

    private Set<Object> suiteCache;

    private Map<String, String> skipTestMethods;

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return suiteCache.stream()
                .anyMatch(o -> parameterContext.getParameter().getType().isAssignableFrom(o.getClass()));
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        final Class<?> type = parameterContext.getParameter().getType();
        return getObject(type);
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
        if (context.getTestMethod().isPresent()) {
            final String currentMethodName = context.getTestMethod().get().getName();
            if (this.skipTestMethods.containsKey(currentMethodName)) {
                return ConditionEvaluationResult.disabled(this.skipTestMethods.get(currentMethodName));
            }
        }
        return ConditionEvaluationResult.enabled("Test enabled");
    }

    @Override
    public void beforeAll(final ExtensionContext extensionContext) {
        final Optional<String> initClassOptional = extensionContext.getConfigurationParameter(INIT_CLASS);
        if (initClassOptional.isPresent()) {
            LOGGER.debug("Initialisation class [{}] found", initClassOptional.get());
            final IntegrationTestSuite integrationTestSuite = getIntegrationTestSuite(initClassOptional.get());
            this.suiteCache = getSuiteObjects(integrationTestSuite);
            this.skipTestMethods = getSkipTestMethods(integrationTestSuite);
        }
    }

    @Override
    public void beforeEach(final ExtensionContext context) {
        context.getRequiredTestInstances()
                .getAllInstances()
                .forEach(this::injectInstanceFields);
    }

    private static Set<Object> getSuiteObjects(final IntegrationTestSuite integrationTestSuite) {
        final Set<Object> objects = integrationTestSuite.getObjects().orElse(Collections.emptySet());
        LOGGER.debug("Retrieved the following objects types from the IntegrationTestSuite: [{}]",
                objects.stream().map(o -> o.getClass().getName()).collect(Collectors.toList()));
        return objects;
    }

    private Map<String, String> getSkipTestMethods(final IntegrationTestSuite integrationTestSuite) {
        final Map<String, String> skipTestMethods = integrationTestSuite.getSkipTestMethods().orElse(Collections.emptyMap());
        LOGGER.debug("Retrieved the following skip-test methods from the IntegrationTestSuite: [{}]",
                StringUtils.join(skipTestMethods));
        return skipTestMethods;
    }

    private void injectInstanceFields(final Object instance) {
        findAnnotatedFields(instance.getClass(), IntegrationTestSuiteInstance.class, ReflectionUtils::isNotStatic).forEach(field -> {
            try {
                LOGGER.debug("Field [{}] requires injecting", field);
                final Object object = getObject(field.getType());
                LOGGER.debug("Object [{}] found for the field", object);
                makeAccessible(field).set(instance, object);
            } catch (final Throwable t) {
                ExceptionUtils.throwAsUncheckedException(t);
            }
        });
    }

    private Object getObject(final Class<?> type) {
        final Set<Object> set = suiteCache.stream()
                .filter(o -> type.isAssignableFrom(o.getClass()))
                .collect(Collectors.toSet());
        if (!set.isEmpty()) {
            return set.iterator().next();
        } else {
            throw new ParameterResolutionException(String.format("Object of type [%s] not found", type));
        }
    }

    private static IntegrationTestSuite getIntegrationTestSuite(final String initClass) {
        final IntegrationTestSuite integrationTestSuite;
        if (INTEGRATION_TEST_SUITE_CLASS_MAP.containsKey(initClass)) {
            integrationTestSuite = INTEGRATION_TEST_SUITE_CLASS_MAP.get(initClass);
        } else {
            synchronized (INTEGRATION_TEST_SUITE_CLASS_MAP) {
                if (INTEGRATION_TEST_SUITE_CLASS_MAP.containsKey(initClass)) {
                    integrationTestSuite = INTEGRATION_TEST_SUITE_CLASS_MAP.get(initClass);
                } else {
                    final Class<?> initialisationClass = tryToLoadClass(initClass).toOptional().orElseThrow(IllegalArgumentException::new);
                    final Object object = ReflectionUtils.newInstance(initialisationClass);
                    if (object instanceof IntegrationTestSuite) {
                        integrationTestSuite = (IntegrationTestSuite) object;
                    } else {
                        throw new ParameterResolutionException(String.format("The object was not op type: [%s]. Actual object type: [%s]",
                                IntegrationTestSuite.class.getName(), object.getClass().getName()));
                    }
                    INTEGRATION_TEST_SUITE_CLASS_MAP.put(initClass, integrationTestSuite);
                }
            }
        }
        return integrationTestSuite;
    }
}
