/*
 * Copyright 2020 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

/**
 * An Operation to get all the graphIds within scope of the FederatedStore.
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.11.0")
@Summary("Gets the ids of all available Graphs from a federated store")
public class GetAllGraphInfo implements
        FederatedOperation,
        Output<Map<String, Object>> {
    private Map<String, String> options;

    public GetAllGraphInfo() {
        addOption(KEY_OPERATION_OPTIONS_GRAPH_IDS, "");
    }

    @Override
    public TypeReference<Map<String, Object>> getOutputTypeReference() {
        return new TypeReferenceImpl.MapStringObject();
    }

    @Override
    public GetAllGraphInfo shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static class Builder extends BaseBuilder<GetAllGraphInfo, Builder> {

        public Builder() {
            super(new GetAllGraphInfo());
        }
    }
}
