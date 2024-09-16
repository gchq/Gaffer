/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * An Operation used for removing graphs from a FederatedStore.
 * <p>Requires:
 * <ul>
 * <li>graphId
 * </ul>
 * Does not delete the graph, just removes it from the scope of the FederatedStore.
 *
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.graph.Graph
 */
@JsonPropertyOrder(value = {"class", "graphId"}, alphabetic = true)
@Since("1.0.0")
@Summary("Removes a Graph from the federated store")
public class RemoveGraph implements IFederationOperation, Output<Boolean> {

    @Required
    private String graphId;
    private Map<String, String> options;
    private boolean userRequestingAdminUsage;
    private boolean removeCache = true;

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    @Override
    public RemoveGraph shallowClone() throws CloneFailedException {
        return new RemoveGraph.Builder()
                .graphId(graphId)
                .options(options)
                .setUserRequestingAdminUsage(userRequestingAdminUsage)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public boolean isUserRequestingAdminUsage() {
        return userRequestingAdminUsage;
    }

    @Override
    public RemoveGraph setUserRequestingAdminUsage(final boolean adminRequest) {
        userRequestingAdminUsage = adminRequest;
        return this;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public TypeReference<Boolean> getOutputTypeReference() {
        return new TypeReferenceImpl.Boolean();
    }

    public boolean isRemoveCache() {
        return removeCache;
    }

    public void setRemoveCache(final boolean removeCache) {
        this.removeCache = removeCache;
    }

    public static class Builder extends IFederationOperation.BaseBuilder<RemoveGraph, Builder> {

        public Builder() {
            super(new RemoveGraph());
        }

        public Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }
        public Builder removeCache(final boolean removeCache) {
            _getOp().setRemoveCache(removeCache);
            return _self();
        }
    }
}
