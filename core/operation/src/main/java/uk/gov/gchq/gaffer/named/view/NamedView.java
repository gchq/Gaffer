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

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;

import java.util.Map;

public class NamedView extends View {

    @Required
    private String viewName;
    private Map<String, Object> parameters;

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

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> extends View.BaseBuilder<CHILD_CLASS> {

        public BaseBuilder() {
            this(new NamedView());
        }

        public BaseBuilder(NamedView namedView) {
            super(namedView);
        }

        public CHILD_CLASS name(final String viewName) {
            getElementDefs().setViewName(viewName);
            return self();
        }

        public CHILD_CLASS parameters(final Map<String, Object> parameters) {
            getElementDefs().setParameters(parameters);
            return self();
        }

        @Override
        public NamedView build() {
            return (NamedView) super.build();
        }

        @Override
        protected NamedView getElementDefs() {
            return (NamedView) super.getElementDefs();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(NamedView namedView) {
            super(namedView);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
