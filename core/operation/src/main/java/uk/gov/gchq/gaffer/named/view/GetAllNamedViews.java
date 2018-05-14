/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.named.view;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.named.view.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@link GetAllNamedViews} is an {@link uk.gov.gchq.gaffer.operation.Operation}
 * for retrieving all {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView}s associated with a Gaffer graph.
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.3.0")
@Summary("Gets all available named views")
public class GetAllNamedViews implements Output<CloseableIterable<NamedViewDetail>> {
    private Map<String, String> options;

    @Override
    public TypeReference<CloseableIterable<NamedViewDetail>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableNamedView();
    }

    @Override
    public GetAllNamedViews shallowClone() throws CloneFailedException {
        return new GetAllNamedViews.Builder()
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

    public static class Builder extends BaseBuilder<GetAllNamedViews, Builder>
            implements Output.Builder<GetAllNamedViews, CloseableIterable<NamedViewDetail>, Builder> {
        public Builder() {
            super(new GetAllNamedViews());
        }
    }
}
