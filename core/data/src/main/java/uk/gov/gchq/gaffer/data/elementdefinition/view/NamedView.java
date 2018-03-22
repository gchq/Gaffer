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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@code NamedView} extends a {@link View}, defining the {@link uk.gov.gchq.gaffer.data.element.Element}s to be returned for an operation.
 * On top of the @{link View}, it contains a name for the {@link View} that is defined.  It also holds a list of {@code NamedView} names,
 * containing all the names of {@code NamedView}'s that have been merged to define the {@code NamedView}.
 *
 * @see Builder
 * @see View
 */
@JsonDeserialize(builder = NamedView.Builder.class)
@JsonPropertyOrder(value = {"class", "name", "edges", "entities"}, alphabetic = true)
public class NamedView extends View {

    @Required
    private String name;
    private Map<String, Object> parameters;
    @JsonIgnore
    private List<String> mergedNamedViewNames;

    public NamedView() {
        this.name = "";
        parameters = new HashMap<>();
        mergedNamedViewNames = new ArrayList<>();
    }

    public void setName(final String viewName) {
        this.name = viewName;
    }

    public String getName() {
        return this.name;
    }

    public void setMergedNamedViewNames(final List<String> mergedNamedViewNames) {
        if (mergedNamedViewNames != null) {
            if (this.mergedNamedViewNames != null) {
                this.mergedNamedViewNames.addAll(mergedNamedViewNames);
            } else {
                this.mergedNamedViewNames = mergedNamedViewNames;
            }
        }
    }

    public List<String> getMergedNamedViewNames() {
        return mergedNamedViewNames;
    }

    public void setParameters(final Map<String, Object> parameters) {
        if (parameters != null) {
            if (null != this.parameters) {
                this.parameters.putAll(parameters);
            } else {
                this.parameters = parameters;
            }
        }
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public boolean canMerge(final View addingView, final View srcView) {
        if (addingView instanceof NamedView && !(srcView instanceof NamedView)) {
            if (((NamedView) addingView).getName() != null) {
                if (!((NamedView) addingView).getName().isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void expandGlobalDefinitions() {
        // As it is a named view - we should not expand the global definitions.
        // It should only be expanded after the named view is resolved.
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> extends View.BaseBuilder<CHILD_CLASS> {

        public BaseBuilder() {
            this(new NamedView());
        }

        public BaseBuilder(final NamedView namedView) {
            super(namedView);
        }

        public CHILD_CLASS name(final String name) {
            getElementDefs().setName(name);
            return self();
        }

        public CHILD_CLASS parameters(final Map<String, Object> parameters) {
            getElementDefs().setParameters(parameters);
            return self();
        }

        @Override
        @JsonIgnore
        public CHILD_CLASS json(final InputStream... inputStreams) throws SchemaException {
            return json(NamedView.class, inputStreams);
        }

        @Override
        @JsonIgnore
        public CHILD_CLASS json(final Path... filePaths) throws SchemaException {
            return json(NamedView.class, filePaths);
        }

        @Override
        @JsonIgnore
        public CHILD_CLASS json(final byte[]... jsonBytes) throws SchemaException {
            return json(NamedView.class, jsonBytes);
        }

        @Override
        public CHILD_CLASS merge(final View view) {
            if (null != view) {
                if (view instanceof NamedView) {
                    NamedView namedViewInstance = (NamedView) view;
                    if (StringUtils.isNotEmpty(namedViewInstance.getName())) {
                        if (StringUtils.isEmpty(self().getElementDefs().getName())) {
                            self().name(namedViewInstance.getName());
                        } else {
                            if (!self().getElementDefs().getName().equals(((NamedView) view).getName())) {
                                self().getElementDefs().getMergedNamedViewNames().add(namedViewInstance.getName());
                            }
                        }
                    }
                    if (CollectionUtils.isNotEmpty(namedViewInstance.getMergedNamedViewNames())) {
                        self().getElementDefs().setMergedNamedViewNames(namedViewInstance.getMergedNamedViewNames());
                    }
                    if (MapUtils.isNotEmpty(namedViewInstance.getParameters())) {
                        self().parameters(namedViewInstance.getParameters());
                    }
                }
                super.merge(view);
            }
            return self();
        }

        @Override
        public NamedView build() {
            if (self().getElementDefs().getName() == null || self().getElementDefs().getName().isEmpty()) {
                throw new IllegalArgumentException("Name must be set");
            }
            return (NamedView) super.build();
        }

        @Override
        protected NamedView getElementDefs() {
            return (NamedView) super.getElementDefs();
        }
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final NamedView namedView) {
            super(namedView);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
