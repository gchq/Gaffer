/*
 * Copyright 2020-2022 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

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
    private String graphIdsCsv;
    private boolean userRequestingAdminUsage;

    @JsonProperty("graphIds")
    public GetAllGraphInfo graphIdsCSV(final String graphIds) {
        this.graphIdsCsv = graphIds;
        return this;
    }

    @JsonProperty("graphIds")
    public String getGraphIdsCSV() {
        return graphIdsCsv;
    }

    @Override
    public TypeReference<Map<String, Object>> getOutputTypeReference() {
        return new TypeReferenceImpl.MapStringObject();
    }

    @Override
    public GetAllGraphInfo shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .graphIDsCSV(graphIdsCsv)
                .userRequestingAdminUsage(userRequestingAdminUsage)
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
                .append(graphIdsCsv, that.graphIdsCsv)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(options)
                .append(graphIdsCsv)
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
    public GetAllGraphInfo isUserRequestingAdminUsage(final boolean adminRequest) {
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
    }
}
