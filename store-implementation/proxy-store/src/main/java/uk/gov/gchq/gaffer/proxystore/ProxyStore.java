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

package uk.gov.gchq.gaffer.proxystore;

import com.fasterxml.jackson.core.type.TypeReference;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.GafferWrappedErrorRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.proxystore.operation.handler.OperationChainHandler;
import uk.gov.gchq.gaffer.proxystore.response.deserialiser.ResponseDeserialiser;
import uk.gov.gchq.gaffer.proxystore.response.deserialiser.impl.DefaultResponseDeserialiser;
import uk.gov.gchq.gaffer.proxystore.response.deserialiser.impl.OperationsResponseDeserialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
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

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

import static java.util.Objects.nonNull;

/**
 * Gaffer {@code ProxyStore} implementation.
 * <p>
 * The ProxyStore is simply a Gaffer store which delegates all operations to a Gaffer
 * REST API.
 */
public class ProxyStore extends Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyStore.class);
    public static final String ERROR_FETCHING_SCHEMA_FROM_REMOTE_STORE = "Error fetching schema from remote store.";
    private Client client;

    public ProxyStore() {
        super(false);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "The properties should always be ProxyProperties")
    @Override
    public void initialise(final String graphId, final Schema unusedSchema, final StoreProperties properties) throws StoreException {
        setProperties(properties);
        client = createClient();

        super.initialise(graphId, new Schema(), getProperties());
        checkDelegateStoreStatus();
    }

    @SuppressWarnings("rawtypes")
    protected void checkDelegateStoreStatus() throws StoreException {
        final URL url = getProperties().getGafferUrl("graph/status");
        final ResponseDeserialiser<LinkedHashMap> responseDeserialiser = getResponseDeserialiserFor(new TypeReferenceImpl.Map());
        final LinkedHashMap<?, ?> status = doGet(url, responseDeserialiser, null);
        LOGGER.info("Delegate REST API status: {}", status.get("status"));
    }

    @SuppressFBWarnings(value = "SIC_INNER_SHOULD_BE_STATIC_ANON")
    protected Set<Class<? extends Operation>> fetchOperations() {
        try {
            final URL url = getProperties().getGafferUrl("graph/operations");
            final ResponseDeserialiser<Set<Class<? extends Operation>>> responseDeserialiser = getOperationsResponseDeserialiser();
            return Collections.unmodifiableSet(doGet(url, responseDeserialiser, null));
        } catch (final StoreException e) {
            throw new GafferRuntimeException("Failed to fetch operations from remote store.", e);
        }
    }

    protected ResponseDeserialiser<Set<Class<? extends Operation>>> getOperationsResponseDeserialiser() {
        return new OperationsResponseDeserialiser();
    }

    protected <O> ResponseDeserialiser<O> getResponseDeserialiserFor(final TypeReference<O> typeReference) {
        return new DefaultResponseDeserialiser<>(typeReference);
    }

    protected ResponseDeserialiser getResponseDeserialiserForNamedOperation(final NamedOperation operation, final Context context) throws OperationException {
        Iterable<NamedOperationDetail> namedOpDetails = executeOpChainViaUrl(OperationChain.wrap(new GetAllNamedOperations()), context);
        for (final NamedOperationDetail detail : namedOpDetails) {
            if (detail.getOperationName().equals(operation.getOperationName())) {
                return getResponseDeserialiserFor(detail.getOperationChainWithDefaultParams().getOutputTypeReference());
            }
        }
        return getResponseDeserialiserFor(operation.getOutputTypeReference());
    }

    @Override
    public Set<Class<? extends Operation>> getSupportedOperations() {
        final HashSet<Class<? extends Operation>> allSupportedOperations = new HashSet<>();
        allSupportedOperations.addAll(fetchOperations());
        allSupportedOperations.addAll(super.getSupportedOperations());
        return Collections.unmodifiableSet(allSupportedOperations);
    }

    @Override
    public boolean isSupported(final Class<? extends Operation> operationClass) {
        final boolean isClassAddNamedViewOrAddNamedOperation = AddNamedView.class.isAssignableFrom(operationClass) || AddNamedOperation.class.isAssignableFrom(operationClass);
        return isClassAddNamedViewOrAddNamedOperation
                ? super.getSupportedOperations().contains(operationClass)
                : getSupportedOperations().contains(operationClass);
    }

    protected Set<StoreTrait> fetchTraits(final Operation operation) throws OperationException {
        try {
            Set<StoreTrait> newTraits = executeOpChainViaUrl(new OperationChain<>(operation), new Context());
            if (newTraits == null) {
                newTraits = new HashSet<>(0);
            } else {
                // This proxy store cannot handle visibility due to the simple rest api using a default user.
                newTraits.remove(StoreTrait.VISIBILITY);
            }
            return newTraits;
        } catch (final Exception e) {
            throw new OperationException("Proxy Store failed to fetch traits from remote store", e);
        }
    }

    protected Schema fetchSchema(final boolean getCompactSchema) throws OperationException {
        final GetSchema.Builder getSchema = new GetSchema.Builder();
        getSchema.compact(getCompactSchema);
        return executeOpChainViaUrl(new OperationChain<>(getSchema.build()), new Context());
    }

    /**
     * Get original {@link Schema} from the remote Store.
     *
     * @return original {@link Schema}
     */
    @Override
    public Schema getOriginalSchema() {
        try {
            return fetchSchema(false);
        } catch (final OperationException e) {
            throw new GafferRuntimeException(ERROR_FETCHING_SCHEMA_FROM_REMOTE_STORE, e);
        }
    }

    /**
     * Get {@link Schema} from the remote Store.
     *
     * @return optimised compact {@link Schema}
     */
    @Override
    public Schema getSchema() {
        try {
            return fetchSchema(true);
        } catch (final OperationException e) {
            throw new GafferRuntimeException(ERROR_FETCHING_SCHEMA_FROM_REMOTE_STORE, e);
        }
    }

    @Override
    public void validateSchemas() {
        // no validation required
    }

    @Override
    public JobDetail executeJob(final OperationChain<?> operationChain, final Context context)
            throws OperationException {
        final URL url = getProperties().getGafferUrl("graph/jobs");
        try {
            final ResponseDeserialiser<JobDetail> responseDeserialiser = getResponseDeserialiserFor(new TypeReferenceImpl.JobDetail());
            return doPost(url, operationChain, responseDeserialiser, context);
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }

    public <O> O executeOpChainViaUrl(final OperationChain<O> opChain, final Context context)
            throws OperationException {
        final String opChainJson;
        try {
            opChainJson = new String(JSONSerialiser.serialise(opChain), StandardCharsets.UTF_8);
        } catch (final SerialisationException e) {
            throw new OperationException("Unable to serialise operation chain into JSON.", e);
        }

        final URL url = getProperties().getGafferUrl("graph/operations/execute");
        try {
            final ResponseDeserialiser<O> responseDeserialiser;
            final Operation lastOp = opChain.getOperations().get(opChain.getOperations().size() - 1);
            if (lastOp instanceof NamedOperation) {
                responseDeserialiser = getResponseDeserialiserForNamedOperation((NamedOperation) lastOp, context);
            } else {
                responseDeserialiser = getResponseDeserialiserFor(opChain.getOutputTypeReference());
            }
            return doPost(url, opChainJson, responseDeserialiser, context);
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }

    protected <O> O doPost(final URL url, final Object body,
                           final ResponseDeserialiser<O> responseDeserialiser,
                           final Context context)
            throws StoreException {
        try {
            return doPost(url, new String(JSONSerialiser.serialise(body), StandardCharsets.UTF_8), responseDeserialiser, context);
        } catch (final SerialisationException e) {
            throw new StoreException("Unable to serialise body of request into json.", e);
        }
    }

    protected <O> O doPost(final URL url, final String jsonBody,
                           final ResponseDeserialiser<O> responseDeserialiser,
                           final Context context)
            throws StoreException {

        final Invocation.Builder request = createRequest(jsonBody, url, context);
        final Response response;
        try {
            response = request.post(Entity.json(jsonBody));
        } catch (final Exception e) {
            throw new StoreException(String.format("Failed to execute post via the Gaffer URL %s", url.toExternalForm()), e);
        }

        return handleResponse(response, responseDeserialiser);
    }

    protected <O> O doGet(final URL url, final ResponseDeserialiser<O> responseDeserialiser, final Context context) throws StoreException {
        final Invocation.Builder request = createRequest(null, url, context);
        final Response response;
        try {
            response = request.get();
        } catch (final Exception e) {
            throw new StoreException(String.format("Request failed to execute via url %s", url.toExternalForm()), e);
        }

        return handleResponse(response, responseDeserialiser);
    }

    protected <O> O handleResponse(final Response response,
                                   final ResponseDeserialiser<O> responseDeserialiser)
            throws StoreException {
        final String outputJson = response.hasEntity() ? response.readEntity(String.class) : null;
        if (Family.SUCCESSFUL != response.getStatusInfo().getFamily()) {
            final Error error;
            try {
                error = JSONSerialiser.deserialise(StringUtil.toBytes(outputJson), Error.class);
            } catch (final Exception e) {
                LOGGER.warn("Gaffer bad status {}. Detail: {}", response.getStatus(), outputJson);
                throw new StoreException(String.format("Delegate Gaffer store returned status: %s. Response content was: %s", response.getStatus(), outputJson), e);
            }
            throw new GafferWrappedErrorRuntimeException(error);
        }

        O output = null;
        if (nonNull(outputJson)) {
            try {
                output = responseDeserialiser.deserialise(outputJson);
            } catch (final SerialisationException e) {
                throw new StoreException(e.getMessage(), e);
            }
        }

        return output;
    }

    protected Invocation.Builder createRequest(final String body, final URL url, final Context context) {
        final Invocation.Builder request = client.target(url.toString())
                .request();
        if (nonNull(body)) {
            request.header("Content", MediaType.APPLICATION_JSON_TYPE);
            request.header("Content-Type", MediaType.APPLICATION_JSON_TYPE);
            request.header("Accept", MediaType.APPLICATION_JSON_TYPE);
            request.build(body);
        }
        return request;
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
        addOperationHandler(OperationChain.class, new OperationChainHandler<>(opChainValidator, opChainOptimisers));
        addOperationHandler(OperationChainDAO.class, new OperationChainHandler<>(opChainValidator, opChainOptimisers));
        addOperationHandler(GetTraits.class, getGetTraitsHandler());
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        // Create an anonymous class (implementing OutputOperationHandler) which calls fetchTraits with the operation
        return (operation, context, store) -> ((ProxyStore) store).fetchTraits(operation);
    }

    @Override
    protected OperationHandler<? extends OperationChain<?>> getOperationChainHandler() {
        return null;
    }

    protected Client createClient() {
        final Client client = ClientBuilder.newClient();
        client.property(ClientProperties.CONNECT_TIMEOUT, getProperties().getConnectTimeout());
        client.property(ClientProperties.READ_TIMEOUT, getProperties().getReadTimeout());
        return client;
    }

    @SuppressWarnings("rawtypes")
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
