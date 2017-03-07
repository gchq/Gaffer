/*
 * Copyright 2016-2017 Crown Copyright
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
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
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
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TypeReferenceStoreImpl;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;


public class ProxyStore extends Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyStore.class);
    private JSONSerialiser jsonSerialiser;
    private Client client;
    private Set<StoreTrait> traits;
    private Schema schema;
    private Set<Class<? extends Operation>> supportedOperations;

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "The properties should always be ProxyProperties")
    @Override
    public void initialise(final Schema unusedSchema, final StoreProperties properties) throws StoreException {
        final ProxyProperties proxyProps = (ProxyProperties) properties;
        jsonSerialiser = proxyProps.getJsonSerialiser();

        client = createClient(proxyProps);
        schema = fetchSchema(proxyProps);
        traits = fetchTraits(proxyProps);
        supportedOperations = fetchOperations(proxyProps);

        super.initialise(schema, proxyProps);
        checkDelegateStoreStatus(proxyProps);
    }

    protected void checkDelegateStoreStatus(final ProxyProperties proxyProps) throws StoreException {
        final URL url = proxyProps.getGafferUrl("status");
        final LinkedHashMap status = doGet(url, new TypeReferenceImpl.Map(), null);
        LOGGER.info("Delegate REST API status: " + status.get("description"));
    }

    protected Set<Class<? extends Operation>> fetchOperations(final ProxyProperties proxyProps) throws StoreException {
        final URL url = proxyProps.getGafferUrl("graph/operations");
        return (Set) Collections.unmodifiableSet(doGet(url, new TypeReferenceImpl.Operations(), null));
    }

    @Override
    public Set<Class<? extends Operation>> getSupportedOperations() {
        return supportedOperations;
    }

    @Override
    public boolean isSupported(final Class<? extends Operation> operationClass) {
        return supportedOperations.contains(operationClass);
    }

    protected Set<StoreTrait> fetchTraits(final ProxyProperties proxyProps) throws StoreException {
        final URL url = proxyProps.getGafferUrl("graph/storeTraits");
        Set<StoreTrait> newTraits = doGet(url, new TypeReferenceStoreImpl.StoreTraits(), null);
        if (null == newTraits) {
            newTraits = new HashSet<>(0);
        } else {
            // This proxy store cannot handle visibility due to the simple rest api using a default user.
            newTraits.remove(StoreTrait.VISIBILITY);
        }
        return newTraits;
    }

    protected Schema fetchSchema(final ProxyProperties proxyProps) throws
            StoreException {
        final URL url = proxyProps.getGafferUrl("graph/schema");
        return doGet(url, new TypeReferenceStoreImpl.Schema(), null);
    }

    @Override
    public JobDetail executeJob(final OperationChain<?> operationChain, final User user) throws OperationException {
        final URL url = getProperties().getGafferUrl("graph/jobs/doOperation");
        try {
            return doPost(url, operationChain, new TypeReferenceImpl.JobDetail(), new Context(user));
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
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
            opChainJson = new String(jsonSerialiser.serialise(operationChain), CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException | SerialisationException e) {
            throw new OperationException("Unable to serialise operation chain into JSON.", e);
        }

        final URL url = getProperties().getGafferUrl("graph/doOperation");
        try {
            return doPost(url, opChainJson, operationChain.getOutputTypeReference(), context);
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }

    protected <OUTPUT> OUTPUT doPost(final URL url, final Object body,
                                     final TypeReference<OUTPUT> outputType,
                                     final Context context) throws StoreException {
        try {
            return doPost(url, new String(jsonSerialiser.serialise(body), CommonConstants.UTF_8), outputType, context);
        } catch (SerialisationException | UnsupportedEncodingException e) {
            throw new StoreException("Unable to serialise body of request into json.", e);
        }
    }

    protected <OUTPUT> OUTPUT doPost(final URL url, final String jsonBody,
                                     final TypeReference<OUTPUT> clazz,
                                     final Context context) throws StoreException {


        final Builder request = createRequest(jsonBody, url, context);
        final Response response;
        try {
            response = request.post(Entity.json(jsonBody));
        } catch (final Exception e) {
            throw new StoreException("Failed to execute post via " +
                    "the Gaffer URL " + url.toExternalForm(), e);
        }

        return handleResponse(response, clazz);
    }

    protected <OUTPUT> OUTPUT doGet(final URL url,
                                    final TypeReference<OUTPUT> outputTypeReference, final Context context)
            throws StoreException {
        final Invocation.Builder request = createRequest(null, url, context);
        final Response response;
        try {
            response = request.get();
        } catch (final Exception e) {
            throw new StoreException("Request failed to execute via url "
                    + url.toExternalForm(), e);
        }

        return handleResponse(response, outputTypeReference);
    }

    protected <OUTPUT> OUTPUT handleResponse(final Response response,
                                             final TypeReference<OUTPUT> outputTypeReference)
            throws StoreException {
        final String outputJson = response.hasEntity() ? response.readEntity(String.class) : null;
        if (200 != response.getStatus() && 204 != response.getStatus()) {
            LOGGER.warn("Gaffer bad status " + response.getStatus());
            LOGGER.warn("Detail: " + outputJson);
            throw new StoreException("Delegate Gaffer store returned status: " + response.getStatus() + ". Response content was: " + outputJson);
        }

        OUTPUT output = null;
        if (null != outputJson) {
            try {
                output = deserialise(outputJson, outputTypeReference);
            } catch (final SerialisationException e) {
                throw new StoreException(e.getMessage(), e);
            }
        }

        return output;
    }

    protected Builder createRequest(final String body, final URL url, final Context context) {
        final Invocation.Builder request = client.target(url.toString())
                .request();
        if (null != body) {
            request.header("Content", MediaType.APPLICATION_JSON_TYPE);
            request.build(body);
        }
        return request;
    }

    protected <OUTPUT> OUTPUT deserialise(final String jsonString,
                                          final TypeReference<OUTPUT> outputTypeReference)
            throws SerialisationException {
        final byte[] jsonBytes;
        try {
            jsonBytes = jsonString.getBytes(CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new SerialisationException(
                    "Unable to deserialise JSON: " + jsonString, e);
        }

        return jsonSerialiser.deserialise(jsonBytes, outputTypeReference);
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

    protected Client createClient(final ProxyProperties proxyProps) {
        final Client client = ClientBuilder.newClient();
        client.property(ClientProperties.CONNECT_TIMEOUT, proxyProps.getConnectTimeout());
        client.property(ClientProperties.READ_TIMEOUT, proxyProps.getReadTimeout());
        return client;
    }
}
