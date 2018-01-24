/*
 * Copyright 2017 Crown Copyright
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

import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;

import java.util.Map;

/**
 * A {@code DeleteNamedView} is an {@link Operation} for removing a
 * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView} from a Gaffer graph.
 */
public class DeleteNamedView implements Operation {

    @Required
    private String viewName;
    private Map<String, String> options;

    public String getViewName() {
        return viewName;
    }

    public void setViewName(final String viewName) {
        this.viewName = viewName;
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
                .name(viewName)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<DeleteNamedView, Builder> {
        public Builder() {
            super(new DeleteNamedView());
        }

        public Builder name(final String name) {
            _getOp().setViewName(name);
            return _self();
        }
    }
}
