/*
 * Copyright 2021 Crown Copyright
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
package uk.gov.gchq.gaffer.proxystore.operation.handler;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.proxystore.operation.GetProxyProperties;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.HashMap;
import java.util.Map;

import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.CONNECT_TIMEOUT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_CONTEXT_ROOT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_HOST;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_PORT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.READ_TIMEOUT;

public class GetProxyPropertiesHandler implements OutputOperationHandler<GetProxyProperties, Map<String, Object>> {

    public static final String URL_INFERRED = "URL_inferred";

    /**
     * This implementation could work with with any store and properties but gets ONLY the Proxy Properties value from the Proxy store.
     *
     * @param operation the {@link GetProxyProperties} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return          the proxy specific properties only.
     * @throws OperationException Error making return string map
     */
    @Override
    public Map<String, Object> doOperation(final GetProxyProperties operation, final Context context, final Store store) throws OperationException {
        try {
            ProxyProperties properties = new ProxyProperties(store.getProperties().getProperties());

            HashMap<String, Object> rtn = new HashMap<>();
            rtn.put(GAFFER_CONTEXT_ROOT, properties.getGafferContextRoot());
            rtn.put(GAFFER_HOST, properties.getGafferHost());
            rtn.put(GAFFER_PORT, String.valueOf(properties.getGafferPort()));
            rtn.put(CONNECT_TIMEOUT, String.valueOf(properties.getConnectTimeout()));
            rtn.put(READ_TIMEOUT, String.valueOf(properties.getReadTimeout()));
            rtn.put(URL_INFERRED, properties.getGafferUrl().toString());
            return rtn;
        } catch (final Exception e) {
            throw new OperationException("Error making return string map", e);
        }
    }
}
