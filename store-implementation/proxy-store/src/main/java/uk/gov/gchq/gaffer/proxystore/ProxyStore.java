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
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TypeReferenceStoreImpl;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Gaffer {@code ProxyStore} implementation.
 * <p>
 * The ProxyStore is simply a Gaffer store which delegates all operations to a Gaffer
 * REST API.
 */
public class ProxyStore extends Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyStore.class);
    private Client client;
    private Set<StoreTrait> traits;
    private Schema schema;
    private Set<Class<? extends Operation>> supportedOperations;

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "The properties should always be ProxyProperties")
    @Override
    public void initialise(final String graphId, final Schema unusedSchema, final StoreProperties properties) throws StoreException {
        setProperties(properties);

        final String jsonSerialiserClass = getProperties().getJsonSerialiserClass();
        if (null != jsonSerialiserClass) {
            JSONSerialiser.update(jsonSerialiserClass, getProperties().getJsonSerialiserModules());
        }
        client = createClient();
        schema = fetchSchema();
        traits = fetchTraits();
        supportedOperations = fetchOperations();

        super.initialise(graphId, schema, getProperties());
        checkDelegateStoreStatus();
    }

    protected void checkDelegateStoreStatus() throws StoreException {
        final URL url = getProperties().getGafferUrl("graph/status");
        final LinkedHashMap status = doGet(url, new TypeReferenceImpl.Map(), null);
        LOGGER.info("Delegate REST API status: {}", status.get("description"));
    }

    @SuppressFBWarnings(value = "SIC_INNER_SHOULD_BE_STATIC_ANON")
    protected Set<Class<? extends Operation>> fetchOperations() throws StoreException {
        final URL url = getProperties().getGafferUrl("graph/operations");
        return (Set) Collections.unmodifiableSet(doGet(url, new TypeReference<Set<Class<Operation>>>() {
        }, null));
    }

    @Override
    public Set<Class<? extends Operation>> getSupportedOperations() {
        return supportedOperations;
    }

    @Override
    public boolean isSupported(final Class<? extends Operation> operationClass) {
        return supportedOperations.contains(operationClass);
    }

    protected Set<StoreTrait> fetchTraits() throws StoreException {
        final URL url = getProperties().getGafferUrl("graph/config/storeTraits");
        Set<StoreTrait> newTraits = doGet(url, new TypeReferenceStoreImpl.StoreTraits(), null);
        if (null == newTraits) {
            newTraits = new HashSet<>(0);
        } else {
            // This proxy store cannot handle visibility due to the simple rest api using a default user.
            newTraits.remove(StoreTrait.VISIBILITY);
        }
        return newTraits;
    }

    protected Schema fetchSchema() throws
            StoreException {
        final URL url = getProperties().getGafferUrl("graph/config/schema");
        return doGet(url, new TypeReferenceStoreImpl.Schema(), null);
    }

    @Override
    public void validateSchemas() {
        // no validation required
    }

    @Override
    public JobDetail executeJob(final OperationChain<?> operationChain, final Context context) throws OperationException {
        final URL url = getProperties().getGafferUrl("graph/jobs");
        try {
            return doPost(url, operationChain, new TypeReferenceImpl.JobDetail(), context);
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }

    public <O> O executeOpChainViaUrl(final OperationChain<O> opChain, final Context context)
            throws OperationException {
        final String opChainJson;
        try {
            opChainJson = new String(JSONSerialiser.serialise(opChain), CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException | SerialisationException e) {
            throw new OperationException("Unable to serialise operation chain into JSON.", e);
        }

        final URL url = getProperties().getGafferUrl("graph/operations/execute");
        try {
            return doPost(url, opChainJson, opChain.getOutputTypeReference(), context);
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }

    protected <O> O doPost(final URL url, final Object body,
                           final TypeReference<O> outputType,
                           final Context context) throws StoreException {
        try {
            return doPost(url, new String(JSONSerialiser.serialise(body), CommonConstants.UTF_8), outputType, context);
        } catch (final SerialisationException | UnsupportedEncodingException e) {
            throw new StoreException("Unable to serialise body of request into json.", e);
        }
    }

    protected <O> O doPost(final URL url, final String jsonBody,
                           final TypeReference<O> clazz,
                           final Context context) throws StoreException {

        final Invocation.Builder request = createRequest(jsonBody, url, context);
        final Response response;
        try {
            response = request.post(Entity.json(jsonBody));
        } catch (final Exception e) {
            throw new StoreException("Failed to execute post via " +
                    "the Gaffer URL " + url.toExternalForm(), e);
        }

        return handleResponse(response, clazz);
    }

    protected <O> O doGet(final URL url,
                          final TypeReference<O> outputTypeReference, final Context context)
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

    protected <O> O handleResponse(final Response response,
                                   final TypeReference<O> outputTypeReference)
            throws StoreException {
        final String outputJson = response.hasEntity() ? response.readEntity(String.class) : null;
        if (Family.SUCCESSFUL != response.getStatusInfo().getFamily()) {
            LOGGER.warn("Gaffer bad status {}", response.getStatus());
            LOGGER.warn("Detail: {}", outputJson);
            throw new StoreException("Delegate Gaffer store returned status: " + response.getStatus() + ". Response content was: " + outputJson);
        }

        O output = null;
        if (null != outputJson) {
            try {
                output = deserialise(outputJson, outputTypeReference);
            } catch (final SerialisationException e) {
                throw new StoreException(e.getMessage(), e);
            }
        }

        return output;
    }

    protected Invocation.Builder createRequest(final String body, final URL url, final Context context) {
        final Invocation.Builder request = client.target(url.toString())
                .request();
        if (null != body) {
            request.header("Content", MediaType.APPLICATION_JSON_TYPE);
            request.build(body);
        }
        return request;
    }

    protected <O> O deserialise(final String jsonString,
                                final TypeReference<O> outputTypeReference)
            throws SerialisationException {
        final byte[] jsonBytes;
        try {
            jsonBytes = jsonString.getBytes(CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new SerialisationException(
                    "Unable to deserialise JSON: " + jsonString, e);
        }

        return JSONSerialiser.deserialise(jsonBytes, outputTypeReference);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be ProxyProperties")
    @Override
    public ProxyProperties getProperties() {
        return (ProxyProperties) super.getProperties();
    }

    @Override
    protected Class<ProxyProperties> getPropertiesClass() {
        return ProxyProperties.class;
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
    protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends OperationChain<?>> getOperationChainHandler() {
        return new uk.gov.gchq.gaffer.proxystore.operation.handler.OperationChainHandler<>();
    }

    protected Client createClient() {
        final Client client = ClientBuilder.newClient();
        client.property(ClientProperties.CONNECT_TIMEOUT, getProperties().getConnectTimeout());
        client.property(ClientProperties.READ_TIMEOUT, getProperties().getReadTimeout());
        return client;
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return ToBytesSerialiser.class;
    }

    public static final class Builder {
        private final ProxyStore store;
        private final ProxyProperties properties;
        private String graphId;

        public Builder() {
            store = new ProxyStore();
            properties = new ProxyProperties();
            properties.setStoreClass(ProxyStore.class);
            properties.setStorePropertiesClass(ProxyProperties.class);
        }

        public Builder host(final String host) {
            properties.setGafferHost(host);
            return this;
        }

        public Builder port(final int port) {
            properties.setGafferPort(port);
            return this;
        }

        public Builder contextRoot(final String contextRoot) {
            properties.setGafferContextRoot(contextRoot);
            return this;
        }

        public Builder connectTimeout(final int timeout) {
            properties.setConnectTimeout(timeout);
            return this;
        }

        public Builder readTimeout(final int timeout) {
            properties.setReadTimeout(timeout);
            return this;
        }

        public Builder jsonSerialiser(final Class<? extends JSONSerialiser> serialiserClass) {
            properties.setJsonSerialiserClass(serialiserClass);
            return this;
        }

        public Builder graphId(final String graphId) {
            this.graphId = graphId;
            return this;
        }

        public ProxyStore build() {
            try {
                store.initialise(graphId, new Schema(), properties);
            } catch (final StoreException e) {
                throw new IllegalArgumentException("The store could not be initialised with the provided properties", e);
            }
            return store;
        }
    }
}
