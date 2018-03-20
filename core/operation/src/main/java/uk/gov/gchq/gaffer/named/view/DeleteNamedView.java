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
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;

import java.util.Map;

/**
 * A {@code DeleteNamedView} is an {@link Operation} for removing a
 * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView} from a Gaffer graph.
 */
@JsonPropertyOrder(value = {"class", "name"}, alphabetic = true)
@Since("1.3.0")
public class DeleteNamedView implements Operation {
    @Required
    private String name;
    private Map<String, String> options;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
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
    public DeleteNamedView shallowClone() throws CloneFailedException {
        return new DeleteNamedView.Builder()
                .name(name)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<DeleteNamedView, Builder> {
        public Builder() {
            super(new DeleteNamedView());
        }

        public Builder name(final String name) {
            _getOp().setName(name);
            return _self();
        }
    }
}
