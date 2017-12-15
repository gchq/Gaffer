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

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;

public class NamedViewDetail implements Serializable {
    private static final long serialVersionUID = -8354836093398004122L;
    private static final String CHARSET_NAME = CommonConstants.UTF_8;
    private String name;
    private String namedView;
    private String description;
    private Map<String, ViewParameterDetail> parameters;

    public NamedViewDetail(final String name, final String namedView, final String description, final Map<String, ViewParameterDetail> parameters) {
        setName(name);
        setNamedView(namedView);
        setDescription(description);
        setParameters(parameters);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNamedView() {
        return namedView;
    }

    public void setNamedView(String namedView) {
        this.namedView = namedView;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, ViewParameterDetail> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, ViewParameterDetail> parameters) {
        if (parameters != null) {
            if (null != this.parameters) {
                this.parameters.putAll(parameters);
            } else {
                this.parameters = parameters;
            }
        }
    }

    @JsonIgnore
    public NamedView getNamedView(final Map<String, Object> executionParams) {
        String thisViewString = namedView;

        if (null != parameters) {
            Set<String> paramKeys = parameters.keySet();

            for (final String paramKey : paramKeys) {
                Object paramValueObj;

                if (null != executionParams && executionParams.keySet().contains(paramKey)) {
                    paramValueObj = executionParams.get(paramKey);
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

    public static final class Builder {
        private String name;
        private String namedView;
        private String description;
        private Map<String, ViewParameterDetail> parameters;

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder namedView(final NamedView namedView) {
            this.namedView = new String(namedView.toCompactJson());
            return this;
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        public Builder parameters(final Map<String, ViewParameterDetail> parameters) {
            this.parameters = parameters;
            return this;
        }

        public NamedViewDetail build() {
            return new NamedViewDetail(name, namedView, description, parameters);
        }
    }
}
