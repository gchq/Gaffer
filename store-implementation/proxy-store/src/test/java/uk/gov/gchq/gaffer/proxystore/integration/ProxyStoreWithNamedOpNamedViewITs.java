/*
 * Copyright 2023-2024 Crown Copyright
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
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.named.view.DeleteNamedView;
import uk.gov.gchq.gaffer.named.view.GetAllNamedViews;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedOperationsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedViewsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.NamedOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;
import static uk.gov.gchq.gaffer.store.StoreProperties.OPERATION_DECLARATIONS_JSON;

import java.util.Collections;
import java.util.Map;

@ExcludeClassNamePatterns({"uk.gov.gchq.gaffer.integration.impl.JoinIT",
        "uk.gov.gchq.gaffer.integration.impl.GeneratorsIT"}) // Skipped because: The output type reference doesn't deserialise the output correctly
@ConfigurationParameter(key = INIT_CLASS, value = "uk.gov.gchq.gaffer.proxystore.integration.ProxyStoreWithNamedOpNamedViewITs")
public class ProxyStoreWithNamedOpNamedViewITs extends AbstractStoreITs {

    private static final ProxyProperties STORE_PROPERTIES = ProxyProperties
            .loadStoreProperties(StreamUtil.openStream(ProxyStoreITs.class, "/mock-proxy-store.properties"));

    private static final Schema SCHEMA = new Schema();

    private static final Map<String, String> TESTS_TO_SKIP =
        Collections.singletonMap("shouldGetElements", "GetElementsIT.shouldGetElements - fails due to potentially incorrect test. See issue #3314");

    ProxyStoreWithNamedOpNamedViewITs() {
        setSchema(SCHEMA);
        try {
            // When cache related operation handlers are added, like below,
            // then you must ensure the cache prefix is the same as the graphId:
            final String proxyGraphId = "integrationTestGraph";
            final String cacheServiceNamedViewSuffix = STORE_PROPERTIES.getCacheServiceNamedViewSuffix(proxyGraphId);
            final String cacheServiceNamedOperationSuffix = STORE_PROPERTIES.getCacheServiceNamedOperationSuffix(proxyGraphId);
            STORE_PROPERTIES.set(OPERATION_DECLARATIONS_JSON, new String(JSONSerialiser.serialise(new OperationDeclarations.Builder()
                    // Named operation
                    .declaration(new OperationDeclaration.Builder().operation(NamedOperation.class).handler(new NamedOperationHandler()).build())
                    .declaration(new OperationDeclaration.Builder().operation(AddNamedOperation.class).handler(new AddNamedOperationHandler(cacheServiceNamedOperationSuffix, true)).build())
                    .declaration(new OperationDeclaration.Builder().operation(GetAllNamedOperations.class).handler(new GetAllNamedOperationsHandler(cacheServiceNamedOperationSuffix)).build())
                    .declaration(new OperationDeclaration.Builder().operation(DeleteNamedOperation.class).handler(new DeleteNamedOperationHandler(cacheServiceNamedOperationSuffix)).build())

                    // Named view
                    .declaration(new OperationDeclaration.Builder().operation(AddNamedView.class).handler(new AddNamedViewHandler(cacheServiceNamedViewSuffix)).build())
                    .declaration(new OperationDeclaration.Builder().operation(GetAllNamedViews.class).handler(new GetAllNamedViewsHandler(cacheServiceNamedViewSuffix)).build())
                    .declaration(new OperationDeclaration.Builder().operation(DeleteNamedView.class).handler(new DeleteNamedViewHandler(cacheServiceNamedViewSuffix)).build())
                    .build())));
        } catch (SerialisationException e) {
            throw new RuntimeException(e);
        }
        setStoreProperties(STORE_PROPERTIES);
        setTestsToSkip(TESTS_TO_SKIP);
    }
}
