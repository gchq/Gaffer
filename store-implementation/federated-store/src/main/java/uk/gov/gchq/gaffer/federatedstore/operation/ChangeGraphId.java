/*
 * Copyright 2017-2022 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.HashMap;
import java.util.Map;

@JsonPropertyOrder(value = {"class", "graphId", "newGraphId"}, alphabetic = true)
@Since("1.12.0")
@Summary("Changes the Id of a graph")
@JsonInclude(Include.NON_DEFAULT)
public class ChangeGraphId implements Output<Boolean>, IFederationOperation {
    @Required
    private String graphId;
    private String newGraphId;
    private Map<String, String> options = new HashMap<>();
    private boolean userRequestingAdminUsage;

    public String getGraphId() {
        return graphId;
    }

    public String getNewGraphId() {
        return newGraphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    public void setNewGraphId(final String newGraphId) {
        this.newGraphId = newGraphId;
    }

    @Override
    public ChangeGraphId shallowClone() throws CloneFailedException {

        return new Builder()
                .graphId(this.graphId)
                .newGraphId(this.newGraphId)
                .options(this.options)
                .userRequestingAdminUsage(this.userRequestingAdminUsage)
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
    public ChangeGraphId isUserRequestingAdminUsage(final boolean adminRequest) {
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

    public static class Builder extends IFederationOperation.BaseBuilder<ChangeGraphId, ChangeGraphId.Builder> {

        public Builder() {
            this(new ChangeGraphId());
        }

        protected Builder(final ChangeGraphId addGraph) {
            super(addGraph);
        }

        public Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        public Builder newGraphId(final String newGraphId) {
            _getOp().setNewGraphId(newGraphId);
            return _self();
        }
    }
}
