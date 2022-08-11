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
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.EventConditions;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;
import static org.junit.platform.testkit.engine.EventConditions.finishedSuccessfully;
import static org.junit.platform.testkit.engine.EventConditions.skippedWithReason;
import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;

class IntegrationTestSuiteExtensionTest {

    public static final String JUNIT_PLATFORM_SUITE = "junit-platform-suite";

    public static final String TEST_STRING = "test";

    public static final Integer TEST_INTEGER = 1234;

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
                .selectors(DiscoverySelectors.selectClass(MethodInjectionTestSuite.class))
                .configurationParameter(INIT_CLASS, MethodInjectionTestSuite.class.getName())
                .execute()
                .testEvents()
                .assertThatEvents()
                .haveExactly(2, EventConditions.event(EventConditions.test(MethodInjectionTestSuite.class.getName()), finishedSuccessfully()))
                .haveExactly(2, EventConditions.event(EventConditions.test(MethodInjectionIT.class.getName()), finishedSuccessfully()))
                .hasSize(4);
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
                .selectors(DiscoverySelectors.selectClass(FieldInjectionTestSuite.class))
                .configurationParameter(INIT_CLASS, FieldInjectionTestSuite.class.getName())
                .execute()
                .testEvents()
                .assertThatEvents()
                .haveExactly(2, EventConditions.event(EventConditions.test(FieldInjectionTestSuite.class.getName()), finishedSuccessfully()))
                .haveExactly(2, EventConditions.event(EventConditions.test(FieldInjectionIT.class.getName()), finishedSuccessfully()))
                .hasSize(4);
    }

    @SelectClasses(value = {InitClassNotSetIT.class})
    static class InitClassNotSetTestSuite extends AbstractTestSuite {
    }

    @ExtendWith(IntegrationTestSuiteExtension.class)
    static class InitClassNotSetIT {
        @Test
        void shouldFail() {
            fail("The test should not be run as the IntegrationTestSuiteExtension fails first as no initClass was set");
        }
    }

    @Test
    void shouldFailInitClassNotSet() {
        EngineTestKit.engine(JUNIT_PLATFORM_SUITE)
                .selectors(DiscoverySelectors.selectClass(InitClassNotSetTestSuite.class))
                .execute()
                .testEvents()
                .failed()
                .assertThatEvents().isEmpty();
    }

    @SelectClasses(value = {TestSkippedIT.class})
    static class TestSkippedTestSuite extends AbstractTestSuite {
        @Override
        public Optional<Map<String, String>> getSkipTestMethods() {
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
                .selectors(DiscoverySelectors.selectClass(TestSkippedTestSuite.class))
                .configurationParameter(INIT_CLASS, TestSkippedTestSuite.class.getName())
                .execute()
                .testEvents()
                .skipped()
                .assertThatEvents()
                .haveExactly(1, EventConditions.event(EventConditions.test(TestSkippedIT.class.getName()), skippedWithReason("skipped test")))
                .hasSize(1);
    }
}
