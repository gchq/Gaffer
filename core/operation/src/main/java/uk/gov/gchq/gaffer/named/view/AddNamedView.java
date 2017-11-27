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

import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.operation.Operation;

import java.util.Map;

public class AddNamedView implements Operation {
    private NamedView namedView = null;
    private boolean overwriteFlag = false;
    private Map<String, ParameterDetail> parameters;
    private Map<String, String> options;

    public void setNamedView(final NamedView namedView) {
        this.namedView = namedView;
    }

    public NamedView getNamedView() {
        return namedView;
    }

    public void setOverwriteFlag(final boolean overwriteFlag) {
        this.overwriteFlag = overwriteFlag;
    }

    public boolean isOverwriteFlag() {
        return overwriteFlag;
    }

    public void setParameters(Map<String, ParameterDetail> parameters) {
        this.parameters = parameters;
    }

    public Map<String, ParameterDetail> getParameters() {
        return parameters;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public AddNamedView shallowClone() throws CloneFailedException {
        return new AddNamedView.Builder()
                .namedView(namedView)
                .overwrite(overwriteFlag)
                .parameters(parameters)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<AddNamedView, Builder> {
        public Builder() {
            super(new AddNamedView());
        }

        public Builder namedView(final NamedView namedView) {
            _getOp().setNamedView(namedView);
            return _self();
        }

        public Builder overwrite(final boolean overwriteFlag) {
            _getOp().setOverwriteFlag(overwriteFlag);
            return _self();
        }

        public Builder overwrite() {
            return overwrite(true);
        }

        public Builder parameters(final Map<String, ParameterDetail> parameters) {
            _getOp().setParameters(parameters);
            return _self();
        }
    }
}
