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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@code NamedView} extends a {@link View}, defining the {@link uk.gov.gchq.gaffer.data.element.Element}s to be returned for an operation.
 * On top of the @{link View}, it contains a name for the {@link View} that is defined.  It also holds a list of {@code NamedView} names,
 * containing all the names of {@code NamedView}'s that have been merged to define the {@code NamedView}.
 *
 * @see Builder
 * @see View
 */
@JsonDeserialize(builder = NamedView.Builder.class)
public class NamedView extends View {

    @Required
    private String name;
    @JsonIgnore
    private List<String> mergedNamedViewNames;
    @JsonIgnore
    private Map<String, Object> parameterValues;
    private Map<String, ViewParameterDetail> parameters;
    private static final String CHARSET_NAME = CommonConstants.UTF_8;

    public NamedView() {
        this.name = "";
        this.mergedNamedViewNames = new ArrayList<>();
        this.parameters = new HashMap<>();
    }

    public void setName(final String viewName) {
        this.name = viewName;
    }

    public String getName() {
        return this.name;
    }

    @JsonIgnore
    public void setMergedNamedViewNames(final List<String> mergedNamedViewNames) {
        if (mergedNamedViewNames != null) {
            if (this.mergedNamedViewNames != null) {
                this.mergedNamedViewNames.addAll(mergedNamedViewNames);
            } else {
                this.mergedNamedViewNames = mergedNamedViewNames;
            }
        }
    }

    @JsonIgnore
    public List<String> getMergedNamedViewNames() {
        return mergedNamedViewNames;
    }

    public void setParameters(final Map<String, ViewParameterDetail> parameters) {
        if (parameters != null) {
            if (null != this.parameters) {
                this.parameters.putAll(parameters);
            } else {
                this.parameters = parameters;
            }
        }
    }

    public Map<String, ViewParameterDetail> getParameters() {
        return parameters;
    }

    @JsonIgnore
    public void setParameterValues(final Map<String, Object> parameterValues) {
        if (parameterValues != null) {
            if (null != this.parameterValues) {
                this.parameterValues.putAll(parameterValues);
            } else {
                this.parameterValues = parameterValues;
            }
        }
    }

    @JsonIgnore
    public Map<String, Object> getParameterValues() {
        return parameterValues;
    }

    @Override
    @JsonIgnore
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

    @JsonIgnore
    public NamedView getNamedView() {
        String thisViewString = new String(this.toCompactJson());

        if (null != parameters) {
            Set<String> paramKeys = parameters.keySet();

            for (String paramKey : paramKeys) {
                Object paramValueObj;

                if (null != parameterValues && parameterValues.keySet().contains(paramKey)) {
                    paramValueObj = parameterValues.get(paramKey);
                } else {
                    if (parameters.get(paramKey).getDefaultValue() != null && !parameters.get(paramKey).isRequired()) {
                        paramValueObj = parameters.get(paramKey).getDefaultValue();
                    } else {
                        throw new IllegalArgumentException("Missing parameter " + paramKey + " with no default");
                    }
                }
                try {
                    thisViewString = thisViewString.replace(buildParamNameString(paramKey),
                            new String(JSONSerialiser.serialise(paramValueObj, CHARSET_NAME), CHARSET_NAME));
                } catch (final SerialisationException | UnsupportedEncodingException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            }
        }

        NamedView namedView;

        try {
            namedView = JSONSerialiser.deserialise(thisViewString.getBytes(CHARSET_NAME), NamedView.class);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        return namedView;
    }

    private String buildParamNameString(final String paramKey) {
        return "\"${" + paramKey + "}\"";
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

        public CHILD_CLASS parameters(final Map<String, ViewParameterDetail> parameters) {
            getElementDefs().setParameters(parameters);
            return self();
        }

        public CHILD_CLASS parameterValues(final Map<String, Object> parameterValues) {
            getElementDefs().setParameterValues(parameterValues);
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
                    if (null != namedViewInstance.getName() && !namedViewInstance.getName().isEmpty()) {
                        if (null == self().getElementDefs().getName() || self().getElementDefs().getName().isEmpty()) {
                            self().name(namedViewInstance.getName());
                        } else {
                            if (self().getElementDefs().getName() != ((NamedView) view).getName()) {
                                self().getElementDefs().getMergedNamedViewNames().add(namedViewInstance.getName());
                            }
                        }
                    }
                    if (null != namedViewInstance.getMergedNamedViewNames() && !namedViewInstance.getMergedNamedViewNames().isEmpty()) {
                        self().getElementDefs().setMergedNamedViewNames(namedViewInstance.getMergedNamedViewNames());
                    }
                    if (null != namedViewInstance.getParameters() && !namedViewInstance.getParameters().isEmpty()) {
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
