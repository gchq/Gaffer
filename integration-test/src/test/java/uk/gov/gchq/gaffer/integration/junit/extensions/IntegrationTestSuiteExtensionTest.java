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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedSuccessfully;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.EventConditions.skippedWithReason;
import static org.junit.platform.testkit.engine.EventConditions.test;
import static org.junit.platform.testkit.engine.EventConditions.uniqueIdSubstring;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.cause;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;

@ExtendWith(MockitoExtension.class)
class IntegrationTestSuiteExtensionTest {

    public static final String JUNIT_PLATFORM_SUITE = "junit-platform-suite";

    public static final Schema TEST_SCHEMA = new Schema();

    public static final StoreProperties TEST_STORE_PROPERTIES = new StoreProperties();

    private static final String ERROR_THE_TEST_SHOULD_NOT_RUN = "The test should not be run as the IntegrationTestSuiteExtension fails first as no initClass was set";

    @SelectClasses(value = {MethodInjectionIT.class})
    static class MethodInjectionTestSuite extends AbstractTestSuite {
        MethodInjectionTestSuite() {
            setSchema(TEST_SCHEMA);
            setStoreProperties(TEST_STORE_PROPERTIES);
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class MethodInjectionIT {
        @Test
        void shouldSucceedMethodInjectionString(@InjectedFromStoreITsSuite final Schema actualSchema) {
            assertThat(actualSchema).isEqualTo(TEST_SCHEMA);
        }

        @Test
        void shouldSucceedMethodInjectionInteger(@InjectedFromStoreITsSuite final StoreProperties actualStoreProperties) {
            assertThat(actualStoreProperties).isEqualTo(TEST_STORE_PROPERTIES);
        }
    }

    @Test
    void shouldSucceedMethodInjectionTest() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(MethodInjectionTestSuite.class))
                .configurationParameter(INIT_CLASS, MethodInjectionTestSuite.class.getName())
                .execute()
                .testEvents()
                .assertThatEvents()
                .haveExactly(2, event(test(MethodInjectionTestSuite.class.getName()), finishedSuccessfully()))
                .haveExactly(2, event(test(MethodInjectionIT.class.getName()), finishedSuccessfully()))
                .hasSize(4);
    }

    @SelectClasses(value = {MethodInjectionObjectNotInSetIT.class})
    static class MethodInjectionTestObjectNotInSetSuite extends AbstractTestSuite {
        MethodInjectionTestObjectNotInSetSuite() {
            setSchema(TEST_SCHEMA);
            setStoreProperties(TEST_STORE_PROPERTIES);
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class MethodInjectionObjectNotInSetIT {
        @SuppressWarnings("unused")
        @Test
        void shouldFail(@InjectedFromStoreITsSuite final Float actualFloat) {
            fail(ERROR_THE_TEST_SHOULD_NOT_RUN);
        }
    }

    @Test
    void shouldFailMethodInjectionTestObjectNotInSet() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(MethodInjectionTestObjectNotInSetSuite.class))
                .configurationParameter(INIT_CLASS, MethodInjectionTestObjectNotInSetSuite.class.getName())
                .execute()
                .testEvents()
                .assertThatEvents()
                .haveExactly(1, event(test(MethodInjectionTestObjectNotInSetSuite.class.getName()),
                        finishedWithFailure(message(String.format("No ParameterResolver registered for parameter [java.lang.Float arg0]" +
                                        " in method [void %s.shouldFail(java.lang.Float)].",
                                MethodInjectionObjectNotInSetIT.class.getName())))))
                .hasSize(2);
    }

    @SelectClasses(value = {FieldInjectionIT.class})
    static class FieldInjectionTestSuite extends AbstractTestSuite {
        FieldInjectionTestSuite() {
            setSchema(TEST_SCHEMA);
            setStoreProperties(TEST_STORE_PROPERTIES);
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class FieldInjectionIT {
        @InjectedFromStoreITsSuite
        Schema actualSchema;
        @InjectedFromStoreITsSuite
        StoreProperties actualStoreProperties;

        @Test
        void shouldSucceedFieldInjectionSchema() {
            assertThat(actualSchema).isEqualTo(TEST_SCHEMA);
        }

        @Test
        void shouldSucceedFieldInjectionInteger() {
            assertThat(actualStoreProperties).isEqualTo(TEST_STORE_PROPERTIES);
        }
    }

    @Test
    void shouldSucceedFieldInjectionTest() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(FieldInjectionTestSuite.class))
                .configurationParameter(INIT_CLASS, FieldInjectionTestSuite.class.getName())
                .execute()
                .testEvents()
                .assertThatEvents()
                .haveExactly(2, event(test(FieldInjectionTestSuite.class.getName()), finishedSuccessfully()))
                .haveExactly(2, event(test(FieldInjectionIT.class.getName()), finishedSuccessfully()))
                .hasSize(4);
    }

    @SelectClasses(value = {FieldInjectionObjectNotInSetIT.class})
    static class FieldInjectionTestObjectNotInSetSuite extends AbstractTestSuite {
        FieldInjectionTestObjectNotInSetSuite() {
            setSchema(TEST_SCHEMA);
            setStoreProperties(TEST_STORE_PROPERTIES);
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class FieldInjectionObjectNotInSetIT {
        @SuppressWarnings("unused")
        @InjectedFromStoreITsSuite
        Float actualFloat;

        @Test
        void shouldFail() {
            fail(ERROR_THE_TEST_SHOULD_NOT_RUN);
        }
    }

    @Test
    void shouldFailFieldInjectionObjectNotInSetTest() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(FieldInjectionTestObjectNotInSetSuite.class))
                .configurationParameter(INIT_CLASS, FieldInjectionTestObjectNotInSetSuite.class.getName())
                .execute()
                .testEvents()
                .assertThatEvents()
                .haveExactly(1, event(test(FieldInjectionTestObjectNotInSetSuite.class.getName()),
                        finishedWithFailure(message("Error accessing the field object"),
                        cause(message("Object of type [class java.lang.Float] not found")))))
                .hasSize(2);
    }

    @SelectClasses(value = {InitClassNotSetIT.class})
    static class InitClassNotSetTestSuite extends AbstractTestSuite {
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class InitClassNotSetIT {
        @Test
        void shouldFail() {
            fail(ERROR_THE_TEST_SHOULD_NOT_RUN);
        }
    }

    @Test
    void shouldFailInitClassNotSet() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(InitClassNotSetTestSuite.class))
                .execute()
                .allEvents()
                .assertThatEvents()
                .haveExactly(1, event(uniqueIdSubstring(IntegrationTestSuiteExtension.class.getName()),
                        finishedWithFailure(message("The initClass @ConfigurationParameter has not been set"))));
    }

    @SelectClasses(value = {InitClassInvalidIT.class})
    static class InitClassInvalidTestSuite extends AbstractTestSuite {
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class InitClassInvalidIT {
        @Test
        void shouldFail() {
            fail(ERROR_THE_TEST_SHOULD_NOT_RUN);
        }
    }

    @Test
    void shouldFailInitClassInvalid() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(InitClassInvalidTestSuite.class))
                .configurationParameter(INIT_CLASS, "fail")
                .execute()
                .allEvents()
                .assertThatEvents()
                .haveExactly(1, event(uniqueIdSubstring(IntegrationTestSuiteExtension.class.getName()),
                        finishedWithFailure(message("A class could not be loaded for initClass [fail]"))));
    }

    @SelectClasses(value = {InitClassNullIT.class})
    static class InitClassNullTestSuite extends AbstractTestSuite {
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class InitClassNullIT {
        @Test
        void shouldFail() {
            fail(ERROR_THE_TEST_SHOULD_NOT_RUN);
        }
    }

    @Test
    void shouldFailInitClassNull() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(InitClassNullTestSuite.class))
                .configurationParameter(INIT_CLASS, null)
                .execute()
                .allEvents()
                .assertThatEvents()
                .haveExactly(1, event(uniqueIdSubstring(IntegrationTestSuiteExtension.class.getName()),
                        finishedWithFailure(message("The initClass @ConfigurationParameter has not been set"))));
    }

    @Suite
    @SelectClasses(value = {InitClassDoesNotImplementIntegrationTestSuiteIT.class})
    static class InitClassDoesNotImplementIntegrationTestSuiteSuite {
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class InitClassDoesNotImplementIntegrationTestSuiteIT {
        @Test
        void shouldFail() {
            fail(ERROR_THE_TEST_SHOULD_NOT_RUN);
        }
    }

    @Test
    void shouldFailInitClassDoesNotImplementIntegrationTestSuite() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(InitClassDoesNotImplementIntegrationTestSuiteSuite.class))
                .configurationParameter(INIT_CLASS, InitClassDoesNotImplementIntegrationTestSuiteSuite.class.getName())
                .execute()
                .allEvents()
                .assertThatEvents()
                .haveExactly(1, event(uniqueIdSubstring(IntegrationTestSuiteExtension.class.getName()),
                        finishedWithFailure(message(String.format("The object was not of required type: [%s]. Actual object type: [%s]",
                                IntegrationTestSuite.class.getName(), InitClassDoesNotImplementIntegrationTestSuiteSuite.class.getName())))));
    }

    @SelectClasses(value = {TestSkippedIT.class})
    static class TestSkippedTestSuite extends AbstractTestSuite {
        TestSkippedTestSuite() {
            setSchema(TEST_SCHEMA);
            setStoreProperties(TEST_STORE_PROPERTIES);
            setTestsToSkip(Collections.singletonMap("shouldSkip", "skipped test"));
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class TestSkippedIT {
        @Test
        void shouldSkip() {
            fail("The test should be skipped");
        }
    }

    @Test
    void shouldSucceedTestSkippedTest() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(TestSkippedTestSuite.class))
                .configurationParameter(INIT_CLASS, TestSkippedTestSuite.class.getName())
                .execute()
                .testEvents()
                .skipped()
                .assertThatEvents()
                .haveExactly(1, event(test(TestSkippedIT.class.getName()), skippedWithReason("skipped test")))
                .hasSize(1);
    }

    static class CallCacheTestSuite extends AbstractTestSuite {

        static int instantiated = 0;

        CallCacheTestSuite() {
            instantiated++;
            setSchema(TEST_SCHEMA);
            setStoreProperties(TEST_STORE_PROPERTIES);
        }
    }

    @Test
    void shouldSucceedCallCache(@Mock final ExtensionContext mockExtensionContext) {
        /* setup */
        final IntegrationTestSuiteExtension integrationTestSuiteExtension = new IntegrationTestSuiteExtension();
        CallCacheTestSuite.instantiated = 0;

        /* mock */
        when(mockExtensionContext.getConfigurationParameter(anyString())).thenReturn(Optional.of(CallCacheTestSuite.class.getName()));

        /* run */
        integrationTestSuiteExtension.beforeAll(mockExtensionContext);
        integrationTestSuiteExtension.beforeAll(mockExtensionContext);

        /* verify */
        assertThat(CallCacheTestSuite.instantiated).isEqualTo(1);
    }
}
