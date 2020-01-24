/*
 * Copyright 2019-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.get;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code GetFromEndpoint} is an {@link Operation} that will fetch data from a provided endpoint.
 */
@JsonPropertyOrder(value = {"class", "endpoint"}, alphabetic = true)
@Since("1.8.0")
@Summary("Gets data from an endpoint")
public class GetFromEndpoint implements Output<String>, Operation {

    @Required
    private String endpoint;

    private Map<String, String> options;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(final String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public GetFromEndpoint shallowClone() throws CloneFailedException {
        return new GetFromEndpoint.Builder()
                .endpoint(endpoint)
                .options(options)
                .build();
    }

    @Override
    public TypeReference<String> getOutputTypeReference() {
        return new TypeReferenceImpl.String();
    }

    public static class Builder extends BaseBuilder<GetFromEndpoint, GetFromEndpoint.Builder> {
        public Builder() {
            super(new GetFromEndpoint());
        }

        public GetFromEndpoint.Builder endpoint(final String endpoint) {
            _getOp().setEndpoint(endpoint);
            return _self();
        }
    }
}
