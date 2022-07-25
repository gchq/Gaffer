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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * An Operation to get all the graphIds within scope of the FederatedStore.
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.0.0")
@Summary("Gets the ids of all available Graphs from a federated store")
public class GetAllGraphIds implements
        IFederationOperation,
        Output<Iterable<? extends String>> {
    private Map<String, String> options;
    private boolean userRequestingAdminUsage;

    @Override
    public TypeReference<Iterable<? extends String>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableString();
    }

    @Override
    public GetAllGraphIds shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .userRequestingAdminUsage(userRequestingAdminUsage)
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
    public GetAllGraphIds isUserRequestingAdminUsage(final boolean adminRequest) {
        userRequestingAdminUsage = adminRequest;
        return this;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static class Builder extends IFederationOperation.BaseBuilder<GetAllGraphIds, Builder> {
        public Builder() {
            super(new GetAllGraphIds());
        }
    }
}
