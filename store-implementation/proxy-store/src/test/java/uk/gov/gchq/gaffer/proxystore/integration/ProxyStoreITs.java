/*
 * Copyright 2016-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.proxystore.integration;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.ExcludeClassNamePatterns;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;

import java.util.Collections;
import java.util.Map;

@ExcludeClassNamePatterns({"uk.gov.gchq.gaffer.integration.impl.JoinIT",
                           "uk.gov.gchq.gaffer.integration.impl.GeneratorsIT"}) // Skipped because: The output type reference doesn't deserialise the output correctly
@ConfigurationParameter(key = INIT_CLASS, value = "uk.gov.gchq.gaffer.proxystore.integration.ProxyStoreITs")
public class ProxyStoreITs extends AbstractStoreITs {

    private static final ProxyProperties STORE_PROPERTIES = ProxyProperties
            .loadStoreProperties(StreamUtil.openStream(ProxyStoreITs.class, "/mock-proxy-store.properties"));

    private static final Schema SCHEMA = new Schema();

    private static final Map<String, String> TESTS_TO_SKIP =
        Collections.singletonMap("shouldGetElements", "GetElementsIT.shouldGetElements - fails due to potentially incorrect test. See issue #3314");

    ProxyStoreITs() {
        setSchema(SCHEMA);
        setStoreProperties(STORE_PROPERTIES);
        setTestsToSkip(TESTS_TO_SKIP);
    }
}
