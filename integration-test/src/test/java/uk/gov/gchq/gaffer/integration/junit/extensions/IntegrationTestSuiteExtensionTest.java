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

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.SetUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.testkit.engine.EngineTestKit;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;

class IntegrationTestSuiteExtensionTest {

    public static final String JUNIT_PLATFORM_SUITE = "junit-platform-suite";

    public static final String TEST_STRING = "test";

    public static final String TEST_STRING_2 = "test 2";

    public static final Integer TEST_INTEGER = 1234;

    private static final String ERROR_THE_TEST_SHOULD_RUN = "The test should not be run as the IntegrationTestSuiteExtension fails first as no initClass was set";

    @SelectClasses(value = {MethodInjectionIT.class})
    static class MethodInjectionTestSuite extends AbstractTestSuite {
        @Override
        public Optional<Set<Object>> getObjects() {
            return Optional.of(SetUtils.unmodifiableSet(TEST_STRING, TEST_INTEGER));
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class MethodInjectionIT {
        @Test
        void shouldSucceedMethodInjectionString(@IntegrationTestSuiteInstance final String actualString) {
            assertThat(actualString).isEqualTo(TEST_STRING);
        }

        @Test
        void shouldSucceedMethodInjectionInteger(@IntegrationTestSuiteInstance final Integer actualInteger) {
            assertThat(actualInteger).isEqualTo(TEST_INTEGER);
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

    @SelectClasses(value = {MethodInjectionTwoObjectsSameTypeSecondUsedIT.class})
    static class MethodInjectionTestTwoObjectsSameTypeSecondUsedSuite extends AbstractTestSuite {
        @Override
        public Optional<Set<Object>> getObjects() {
            return Optional.of(SetUtils.unmodifiableSet(TEST_STRING, TEST_STRING_2));
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class MethodInjectionTwoObjectsSameTypeSecondUsedIT {
        @Test
        void shouldSucceedMethodInjectionString(@IntegrationTestSuiteInstance final String actualString) {
            assertThat(actualString).isEqualTo(TEST_STRING_2);
        }
    }

    @Test
    void shouldSucceedMethodInjectionTestTwoObjectsSameTypeSecondUsed() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(MethodInjectionTestTwoObjectsSameTypeSecondUsedSuite.class))
                .configurationParameter(INIT_CLASS, MethodInjectionTestTwoObjectsSameTypeSecondUsedSuite.class.getName())
                .execute()
                .testEvents()
                .assertThatEvents()
                .haveExactly(1, event(test(MethodInjectionTestTwoObjectsSameTypeSecondUsedSuite.class.getName()), finishedSuccessfully()))
                .haveExactly(1, event(test(MethodInjectionTwoObjectsSameTypeSecondUsedIT.class.getName()), finishedSuccessfully()))
                .hasSize(2);
    }

    @SelectClasses(value = {MethodInjectionObjectNotInSetIT.class})
    static class MethodInjectionTestObjectNotInSetSuite extends AbstractTestSuite {
        @Override
        public Optional<Set<Object>> getObjects() {
            return Optional.of(SetUtils.unmodifiableSet(TEST_STRING, TEST_INTEGER));
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class MethodInjectionObjectNotInSetIT {
        @SuppressWarnings("unused")
        @Test
        void shouldFail(@IntegrationTestSuiteInstance final Float actualFloat) {
            fail(ERROR_THE_TEST_SHOULD_RUN);
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
        @Override
        public Optional<Set<Object>> getObjects() {
            return Optional.of(SetUtils.unmodifiableSet(TEST_STRING, TEST_INTEGER));
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class FieldInjectionIT {
        @IntegrationTestSuiteInstance
        String actualString;
        @IntegrationTestSuiteInstance
        Integer actualInteger;

        @Test
        void shouldSucceedFieldInjectionString() {
            assertThat(actualString).isEqualTo(TEST_STRING);
        }

        @Test
        void shouldSucceedFieldInjectionInteger() {
            assertThat(actualInteger).isEqualTo(TEST_INTEGER);
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

    @SelectClasses(value = {FieldInjectionTwoObjectsSameTypeSecondUsedIT.class})
    static class FieldInjectionTestTwoObjectsSameTypeSecondUsedSuite extends AbstractTestSuite {
        @Override
        public Optional<Set<Object>> getObjects() {
            return Optional.of(SetUtils.unmodifiableSet(TEST_STRING, TEST_STRING_2));
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class FieldInjectionTwoObjectsSameTypeSecondUsedIT {
        @IntegrationTestSuiteInstance
        String actualString;

        @Test
        void shouldSucceedFieldInjectionString() {
            assertThat(actualString).isEqualTo(TEST_STRING_2);
        }
    }

    @Test
    void shouldSucceedFieldInjectionTestTwoObjectsSameTypeSecondUsed() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(selectClass(FieldInjectionTestTwoObjectsSameTypeSecondUsedSuite.class))
                .configurationParameter(INIT_CLASS, FieldInjectionTestTwoObjectsSameTypeSecondUsedSuite.class.getName())
                .execute()
                .testEvents()
                .assertThatEvents()
                .haveExactly(1, event(test(FieldInjectionTestTwoObjectsSameTypeSecondUsedSuite.class.getName()), finishedSuccessfully()))
                .haveExactly(1, event(test(FieldInjectionTwoObjectsSameTypeSecondUsedIT.class.getName()), finishedSuccessfully()))
                .hasSize(2);
    }

    @SelectClasses(value = {FieldInjectionObjectNotInSetIT.class})
    static class FieldInjectionTestObjectNotInSetSuite extends AbstractTestSuite {
        @Override
        public Optional<Set<Object>> getObjects() {
            return Optional.of(SetUtils.unmodifiableSet(TEST_STRING, TEST_INTEGER));
        }
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class FieldInjectionObjectNotInSetIT {
        @SuppressWarnings("unused")
        @IntegrationTestSuiteInstance
        Float actualFloat;

        @Test
        void shouldFail() {
            fail(ERROR_THE_TEST_SHOULD_RUN);
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
            fail(ERROR_THE_TEST_SHOULD_RUN);
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
            fail(ERROR_THE_TEST_SHOULD_RUN);
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
            fail(ERROR_THE_TEST_SHOULD_RUN);
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
            fail(ERROR_THE_TEST_SHOULD_RUN);
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
        @Override
        public Optional<Map<String, String>> getTestsToSkip() {
            return Optional.of(MapUtils.unmodifiableMap(Collections.singletonMap("shouldSkip", "skipped test")));
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
}
