/*
 * Copyright 2020-2024 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Gets graph info of selected Graphs from the FederatedStore.
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.11.0")
@Summary("Gets graph info of selected Graphs from the FederatedStore")
public class GetAllGraphInfo implements
        Output<Map<String, Object>>,
        IFederationOperation,
        IFederatedOperation {
    private Map<String, String> options;
    private List<String> graphIds;
    private List<String> blackListGraphsIds;
    private boolean userRequestingAdminUsage;

    @Override
    public GetAllGraphInfo graphIds(final List<String> graphsIds) {
        this.graphIds = graphsIds == null ? null : Collections.unmodifiableList(graphsIds);
        return this;
    }

    @Override
    public IFederatedOperation blackListGraphIds(final List<String> blackListGraphsIds) {
        this.blackListGraphsIds = blackListGraphsIds == null ? null : Collections.unmodifiableList(blackListGraphsIds);
        return this;
    }

    @Override
    @JsonIgnore
    public GetAllGraphInfo graphIdsCSV(final String graphIds) {
        return graphIds(FederatedStoreUtil.getCleanStrings(graphIds));
    }

    @Override
    @JsonIgnore
    public IFederatedOperation blackListGraphIdsCSV(final String blackListGraphIds) {
        return blackListGraphIds(FederatedStoreUtil.getCleanStrings(blackListGraphIds));
    }

    @Override
    @JsonProperty("graphIds")
    public List<String> getGraphIds() {
        return (graphIds == null) ? null : Lists.newArrayList(graphIds);
    }

    @Override
    @JsonProperty("blackListGraphsIds")
    public List<String> getBlackListGraphIds() {
        return (blackListGraphsIds == null) ? null : Lists.newArrayList(blackListGraphsIds);
    }

    @Override
    public TypeReference<Map<String, Object>> getOutputTypeReference() {
        return new TypeReferenceImpl.MapStringObject();
    }

    @Override
    public GetAllGraphInfo shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .graphIDs(graphIds)
                .setUserRequestingAdminUsage(userRequestingAdminUsage)
                .build();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GetAllGraphInfo that = (GetAllGraphInfo) o;

        return new EqualsBuilder()
                .append(options, that.options)
                .append(graphIds, that.graphIds)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(options)
                .append(graphIds)
                .toHashCode();
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
    public GetAllGraphInfo setUserRequestingAdminUsage(final boolean adminRequest) {
        userRequestingAdminUsage = adminRequest;
        return this;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static class Builder extends IFederationOperation.BaseBuilder<GetAllGraphInfo, Builder> {

        public Builder() {
            super(new GetAllGraphInfo());
        }

        public Builder graphIDsCSV(final String graphIdsCSV) {
            this._getOp().graphIdsCSV(graphIdsCSV);
            return this;
        }

        public Builder blackListGraphIdsCSV(final String blackListGraphIdsCSV) {
            this._getOp().blackListGraphIdsCSV(blackListGraphIdsCSV);
            return this;
        }
        public Builder graphIDs(final List<String> graphIds) {
            this._getOp().graphIds(graphIds);
            return this;
        }

        public Builder blackListGraphIds(final List<String> blackListGraphIds) {
            this._getOp().blackListGraphIds(blackListGraphIds);
            return this;
        }
    }
}
