/*
 * Copyright 2021-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.proxystore.operation;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.HashMap;
import java.util.Map;

@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.17.1")
@Summary("Gets ONLY the Proxy Properties value from the Proxy store")
public class GetProxyProperties implements Output<Map<String, Object>> {

    private HashMap<String, String> options = new HashMap<>();

    @Override
    public TypeReference<Map<String, Object>> getOutputTypeReference() {
        return new TypeReferenceImpl.MapStringObject();
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new GetProxyProperties.Builder().options(options).build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = new HashMap<>(options);
    }

    @Override
    public boolean equals(final Object o) {
        return this == o
                || (o != null
                && getClass() == o.getClass()
                && new EqualsBuilder()
                .append(options, ((GetProxyProperties) o).options)
                .isEquals());
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(options)
                .toHashCode();
    }

    public static class Builder extends BaseBuilder<GetProxyProperties, GetProxyProperties.Builder> {
        public Builder() {
            super(new GetProxyProperties());
        }
    }
}
