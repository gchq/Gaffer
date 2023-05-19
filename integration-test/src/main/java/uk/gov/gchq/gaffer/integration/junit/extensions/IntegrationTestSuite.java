/*
 * Copyright 2022-2023 Crown Copyright
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

import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Map;
import java.util.Optional;

/**
 * Getter/setter class. Used as base class for integration test suites.
 * For further information on how this class is used see
 * {@link IntegrationTestSuiteExtension}.
 */
public class IntegrationTestSuite {

    private Schema schema = null;

    private StoreProperties storeProperties = null;

    private Map<String, String> testsToSkip = null;

    /**
     * Returns the {@link Optional} {@link Schema}
     *
     * @return an {@link Optional} {@link Schema}
     */
    public Optional<Schema> getSchema() {
        return Optional.ofNullable(schema);
    }

    /**
     * Sets the {@link Optional} {@link Schema}
     *
     * @param schema the {@link Schema}
     */
    protected void setSchema(final Schema schema) {
        this.schema = schema;
    }


    /**
     * Returns the {@link Optional} {@link StoreProperties}
     *
     * @return an {@link Optional} {@link StoreProperties}
     */
    public Optional<StoreProperties> getStoreProperties() {
        return Optional.ofNullable(storeProperties);
    }

    /**
     * Sets the {@link StoreProperties}
     *
     * @param storeProperties the {@link StoreProperties}
     */
    protected void setStoreProperties(final StoreProperties storeProperties) {
        this.storeProperties = storeProperties;
    }

    /**
     * Returns the {@link Optional} {@link Map} of {@code tests-to-skip}
     *
     * @return an {@link Optional} {@link Map} of test methods to skip together
     * with the reason for skipping the test
     */
    public Optional<Map<String, String>> getTestsToSkip() {
        return Optional.ofNullable(testsToSkip);
    }

    /**
     * Sets the {@link Optional} {@link Map} of tests to skip together with the
     * reason the skipping
     *
     * @param testsToSkip the {@link Map} of tests to skip and the reason
     */
    protected void setTestsToSkip(final Map<String, String> testsToSkip) {
        this.testsToSkip = testsToSkip;
    }
}
