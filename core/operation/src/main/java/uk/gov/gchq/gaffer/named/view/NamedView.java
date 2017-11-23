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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;

import java.util.Map;

public class NamedView extends View implements Operation {

    @Required
    private String viewName;
    private Map<String, Object> parameters;
    private Map<String, String> options;

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public String getViewName() {
        return this.viewName;
    }

    public void setParameters(final Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public Map<String, Object> getParameters() {
        return parameters;
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
    public Operation shallowClone() throws CloneFailedException {
        return new NamedView.Builder()
                .name(viewName)
                .parameters(parameters)
                .options(options)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<NamedView, Builder> {
        public Builder() {
            super(new NamedView());
        }

        public Builder name(final String viewName) {
            _getOp().setViewName(viewName);
            return _self();
        }

        public Builder parameters(final Map<String, Object> parameters) {
            _getOp().setParameters(parameters);
            return _self();
        }
    }
}
