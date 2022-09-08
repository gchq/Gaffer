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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Classes implementing the {@code IntegrationTestSuite} {@code Interface} must
 * override the {@link #getObjects()} and {@link #getTestsToSkip()} methods.
 * Further information on how this class is used can be found at the
 * {@link IntegrationTestSuiteExtension}.
 */
public interface IntegrationTestSuite {

    /**
     * Returns an {@link Optional} {@link Object} {@link Set} of values used by
     * test classes during the execution of the test
     * {@link org.junit.platform.suite.api.Suite}
     *
     * @return an {@link Optional} {@link Object} {@link Set}
     */
    Optional<Set<Object>> getObjects();

    /**
     * Returns an {@link Optional} {@link Map} of {@code tests-to-skip} that is
     * used during the test {@link org.junit.platform.suite.api.Suite} execution
     * to omit the tests specified.
     *
     * @return an {@link Optional} {@link Map} of
     * {@link org.junit.jupiter.api.Test} methods to skip together with the
     * reason for skipping the test
     */
    Optional<Map<String, String>> getTestsToSkip();
}
