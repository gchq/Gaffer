/*
 * Copyright 2023 Crown Copyright
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
import org.junit.platform.suite.api.IncludeClassNamePatterns;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;

@ExcludeClassNamePatterns({"uk.gov.gchq.gaffer.integration.impl.JoinIT",
                           "uk.gov.gchq.gaffer.integration.impl.GeneratorsIT"}) // Skipped because: The output type reference doesn't deserialise the output correctly
@ConfigurationParameter(key = INIT_CLASS, value = "uk.gov.gchq.gaffer.proxystore.integration.ProxyStoreWithNamedOpNamedViewITs")
@IncludeClassNamePatterns(".*IT")
public class ProxyStoreWithNamedOpNamedViewITs extends AbstractStoreITs {

    private static final ProxyProperties STORE_PROPERTIES = ProxyProperties
            .loadStoreProperties(StreamUtil.openStream(ProxyStoreWithNamedOpNamedViewITs.class, "/mock-proxy-store-with-namedop-namedview.properties"));

    private static final Schema SCHEMA = new Schema();

    ProxyStoreWithNamedOpNamedViewITs() {
        setSchema(SCHEMA);
        setStoreProperties(STORE_PROPERTIES);
    }
}
