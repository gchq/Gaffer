/*
 * Copyright 2016-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.integration;

import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

import uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuite;

/**
 * Runs the full suite of gaffer store integration tests.
 * To run the tests against a specific store, extend this class and add
 * '@ConfigurationParameter(key = "initClass", value = "FQCN.of.your.class")'
 * as an annotation. This is required for JUnit to initialise values used
 * by the tests (e.g. Store Properties). You can use annotations from
 * {@link org.junit.platform.suite.api} to include/exclude integration test
 * classes or select additional classes to add to the test Suite.
 */

@Suite
@SelectPackages("uk.gov.gchq.gaffer.integration.impl")
@IncludeClassNamePatterns(".*IT")
public abstract class AbstractStoreITs implements IntegrationTestSuite {

}
