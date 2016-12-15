/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.proxystore;

import com.fasterxml.jackson.core.type.TypeReference;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.http.HttpStatus;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import javax.ws.rs.core.MediaType;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;


public class ProxyStore extends Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyStore.class);
    private static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    private Set<StoreTrait> traits;
    private Schema schema;

    public static class TypeReferenceSchema extends TypeReference<Schema> {
    }

    public static class TypeReferenceStoreTraits extends TypeReference<Set<StoreTrait>> {
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "The properties should always be ProxyProperties")
    @Override
    public void initialise(final Schema unusedSchema, final StoreProperties properties) throws StoreException {
        final ProxyProperties proxyProps = (ProxyProperties) properties;
        fetchSchema(proxyProps);
        fetchTraits(proxyProps);

        super.initialise(schema, proxyProps);
    }

    protected void fetchTraits(final ProxyProperties proxyProps) throws StoreException {
        final URL url = proxyProps.getGafferUrl("graph/storeTraits");
        traits = doGet(url, new TypeReferenceStoreTraits());
        if (null == traits) {
            traits = new HashSet<>(0);
        }
    }

    protected void fetchSchema(final ProxyProperties proxyProps) throws
            StoreException {
        final URL url = proxyProps.getGafferUrl("graph/schema");
        schema = doGet(url, new TypeReferenceSchema());
    }

    @Override
    protected <OUTPUT> OUTPUT handleOperationChain(
            final OperationChain<OUTPUT> operationChain, final Context context)
            throws OperationException {
        return executeOpChainViaUrl(operationChain, context);
    }

    protected <OUTPUT> OUTPUT executeOpChainViaUrl(
            final OperationChain<OUTPUT> operationChain, final Context context)
            throws OperationException {
        final String opChainJson;
        try {
            opChainJson = new String(JSON_SERIALISER.serialise(operationChain), CommonConstants.UTF_8);
        } catch (UnsupportedEncodingException | SerialisationException e) {
            throw new OperationException("Unable to serialise operation chain into JSON.", e);
        }

        final URL url = getProperties().getGafferUrl("graph/doOperation");
        try {
            return doPost(url, opChainJson, operationChain.getTypeReference(), context);
        } catch (StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }

    protected <OUTPUT> OUTPUT doPost(final URL url, final String jsonBody,
                                     final TypeReference<OUTPUT> outputTypeReference,
                                     final Context context) throws StoreException {
        final ClientRequest request = createRequest(jsonBody, url, context);
        final ClientResponse<String> response;
        try {
            response = request.post(String.class);
        } catch (Exception e) {
            throw new StoreException("Failed to execute post via " +
                    "the Gaffer URL " + url.toExternalForm(), e);
        }

        return handleResponse(response, outputTypeReference);
    }

    protected <OUTPUT> OUTPUT doGet(final URL url,
                                    final TypeReference<OUTPUT> outputTypeReference)
            throws StoreException {
        final ClientRequest request = createRequest(null, url, null);
        final ClientResponse<String> response;
        try {
            response = request.get(String.class);
        } catch (Exception e) {
            throw new StoreException("Request failed to execute via url "
                    + url.toExternalForm(), e);
        }

        return handleResponse(response, outputTypeReference);
    }

    protected <OUTPUT> OUTPUT handleResponse(final ClientResponse<String> response,
                                             final TypeReference<OUTPUT> outputTypeReference)
            throws StoreException {
        String outputJson = null;
        switch (response.getStatus()) {
            case HttpStatus.SC_OK:
                outputJson = response.getEntity(String.class);
                break;
            case HttpStatus.SC_NO_CONTENT:
                break;
            default:
                LOGGER.warn("Gaffer bad status " + response.getStatus());
                throw new StoreException("Gaffer bad status " +
                        response.getStatus());
        }

        OUTPUT output = null;
        if (null != outputJson) {
            try {
                output = deserialise(outputJson, outputTypeReference);
            } catch (SerialisationException e) {
                throw new StoreException(e.getMessage(), e);
            }
        }

        return output;
    }

    protected ClientRequest createRequest(final String body, final URL url, final Context context) {
        final ClientRequest request = new ClientRequest(url.toString());
        if (null != body) {
            request.body(MediaType.APPLICATION_JSON_TYPE, body);
        }
        return request;
    }

    protected <OUTPUT> OUTPUT deserialise(final String jsonString,
                                          final TypeReference<OUTPUT> typeReference)
            throws SerialisationException {
        final byte[] jsonBytes;
        try {
            jsonBytes = jsonString.getBytes(CommonConstants.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(
                    "Unable to deserialise JSON: " + jsonString, e);
        }

        return JSON_SERIALISER.deserialise(jsonBytes, typeReference);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be ProxyProperties")
    @Override
    public ProxyProperties getProperties() {
        return (ProxyProperties) super.getProperties();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        // no operation handlers to add.
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return traits;
    }

    @Override
    public boolean isValidationRequired() {
        return false;
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> getGetElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> getGetAllElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
        return null;
    }

    @Override
    protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context) {
        throw new UnsupportedOperationException("All operations should be executed via the provided Gaffer URL");
    }
}
