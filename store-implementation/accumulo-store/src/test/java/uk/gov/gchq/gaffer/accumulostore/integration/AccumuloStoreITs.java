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

package uk.gov.gchq.gaffer.accumulostore.integration;

import org.apache.commons.collections4.SetUtils;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.SelectClasses;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hdfs.integration.loader.AddElementsFromHdfsLoaderIT;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;

@ConfigurationParameter(key = INIT_CLASS, value = "uk.gov.gchq.gaffer.accumulostore.integration.AccumuloStoreITs")
@SelectClasses(AddElementsFromHdfsLoaderIT.class) // Extra test to add
public class AccumuloStoreITs extends AbstractStoreITs {

    private static final AccumuloProperties STORE_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreITs.class));

    private static final Set<Object> OBJECTS = SetUtils.unmodifiableSet(new Schema(), STORE_PROPERTIES);

    private static final Map<String, String> SKIP_TEST_METHODS = Collections.emptyMap();

    @Override
    public Optional<Set<Object>> getObjects() {
        return Optional.of(OBJECTS);
    }

    @Override
    public Optional<Map<String, String>> getTestsToSkip() {
        return Optional.of(SKIP_TEST_METHODS);
    }
}
