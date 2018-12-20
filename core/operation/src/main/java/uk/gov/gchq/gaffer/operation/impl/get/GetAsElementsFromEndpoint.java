/*
 * Copyright 2018 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;
import java.util.function.Function;

/**
 * A {@code GetAsElementsFromEndpoint} is an {@link Operation} that will use the provided {@code ElementGenerator} to
 * convert a String (JSON or otherwise) from a specified endpoint.
 */
@JsonPropertyOrder(value = {"class", "endpoint", "elementGenerator"}, alphabetic = true)
@Since("1.8.0")
@Summary("Gets as Elements from an endpoint")
public class GetAsElementsFromEndpoint implements Output<CloseableIterable<? extends Element>>, Operation {

    @Required
    private String endpoint;
    @Required
    private Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator;
    private Map<String, String> options;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(final String endpoint) {
        this.endpoint = endpoint;
    }

    public Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> getElementGenerator() {
        return elementGenerator;
    }

    public void setElementGenerator(final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator) {
        this.elementGenerator = elementGenerator;
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
    public GetAsElementsFromEndpoint shallowClone() throws CloneFailedException {
        return new GetAsElementsFromEndpoint.Builder()
                .endpoint(endpoint)
                .generator(elementGenerator)
                .options(options)
                .build();
    }

    @Override
    public TypeReference<CloseableIterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableElement();
    }

    public static class Builder extends BaseBuilder<GetAsElementsFromEndpoint, GetAsElementsFromEndpoint.Builder> {
        public Builder() {
            super(new GetAsElementsFromEndpoint());
        }

        public GetAsElementsFromEndpoint.Builder generator(final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }

        public GetAsElementsFromEndpoint.Builder endpoint(final String endpoint) {
            _getOp().setEndpoint(endpoint);
            return _self();
        }
    }
}
