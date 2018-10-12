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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simple POJO containing the details associated with a {@link NamedView}.
 */
@JsonPropertyOrder(value = {"name", "description", "creatorId", "writeAccessRoles", "parameters", "view"}, alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class NamedViewDetail implements Serializable {
    private static final long serialVersionUID = -8354836093398004122L;
    private static final String CHARSET_NAME = CommonConstants.UTF_8;
    private String name;
    private String view;
    private String description;
    private String creatorId;
    private List<String> writeAccessRoles;
    private Map<String, ViewParameterDetail> parameters = Maps.newHashMap();

    public NamedViewDetail() {
    }

    public NamedViewDetail(final String name, final String view, final String description, final Map<String, ViewParameterDetail> parameters) {
        setName(name);
        setView(view);
        setDescription(description);
        this.creatorId = null;
        this.writeAccessRoles = new ArrayList<>();
        setParameters(parameters);
    }

    public NamedViewDetail(final String name, final String view, final String description, final String userId, final List<String> writers, final Map<String, ViewParameterDetail> parameters) {
        setName(name);
        setView(view);
        setDescription(description);
        this.creatorId = userId;
        this.writeAccessRoles = writers;
        setParameters(parameters);
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        if (null != name && !name.isEmpty()) {
            this.name = name;
        } else {
            throw new IllegalArgumentException("Name cannot be null or empty");
        }
    }

    public String getView() {
        return view;
    }

    public void setView(final String view) {
        if (null != view) {
            this.view = view;
        } else {
            throw new IllegalArgumentException("View cannot be null");
        }
    }

    public void setView(final View view) {
        if (null != view) {
            try {
                this.view = new String(JSONSerialiser.serialise(view), Charset.forName(CHARSET_NAME));
            } catch (final SerialisationException se) {
                throw new IllegalArgumentException(se.getMessage());
            }
        } else {
            throw new IllegalArgumentException("View cannot be null");
        }
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public String getCreatorId() {
        return creatorId;
    }

    public List<String> getWriteAccessRoles() {
        return writeAccessRoles;
    }

    public boolean hasWriteAccess(final String userId, final Set<String> opAuths, final String adminAuth) {
        return hasWriteAccess(userId, opAuths, writeAccessRoles, adminAuth);
    }

    public Map<String, ViewParameterDetail> getParameters() {
        return parameters;
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

    /**
     * Gets the View after adding in default values for any parameters. If a parameter
     * does not have a default, null is inserted.
     *
     * @return The {@link View}
     * @throws IllegalArgumentException if substituting the parameters fails
     */
    @JsonIgnore
    public View getViewWithDefaultParams() {
        String viewStringWithDefaults = view;

        if (null != parameters) {
            for (final Map.Entry<String, ViewParameterDetail> parameterDetailPair : parameters.entrySet()) {
                String paramKey = parameterDetailPair.getKey();

                try {
                    viewStringWithDefaults = viewStringWithDefaults.replace(buildParamNameString(paramKey),
                            StringUtil.toString(JSONSerialiser.serialise(parameterDetailPair.getValue().getDefaultValue())));
                } catch (final SerialisationException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            }
        }

        View view;

        try {
            view = JSONSerialiser.deserialise(StringUtil.toBytes(viewStringWithDefaults), View.class);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        return view;
    }

    /**
     * Gets the View after adding in the parameters specified.  If a parameter does
     * not have a default and none is set an Exception will be thrown.
     *
     * @param executionParams Parameters to add
     * @return the {@link View} with substituted parameters
     * @throws IllegalArgumentException if substituting the parameters fails
     */
    public View getView(final Map<String, Object> executionParams) {
        String viewString = view;

        for (final Map.Entry<String, ViewParameterDetail> entry : parameters.entrySet()) {
            final String paramKey = entry.getKey();
            final ViewParameterDetail paramDetail = entry.getValue();

            final Object paramValueObj;
            if (null != executionParams && executionParams.keySet().contains(paramKey)) {
                paramValueObj = executionParams.get(paramKey);
            } else {
                if (null != paramDetail.getDefaultValue() && !paramDetail.isRequired()) {
                    paramValueObj = paramDetail.getDefaultValue();
                } else {
                    throw new IllegalArgumentException("Missing parameter " + paramKey + " with no default");
                }
            }
            try {
                viewString = viewString.replace(buildParamNameString(paramKey),
                        StringUtil.toString(JSONSerialiser.serialise(paramValueObj)));
            } catch (final SerialisationException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        View view;

        try {
            view = JSONSerialiser.deserialise(StringUtil.toBytes(viewString), View.class);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        return view;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final NamedViewDetail op = (NamedViewDetail) obj;

        return new EqualsBuilder()
                .append(name, op.name)
                .append(view, op.view)
                .append(description, op.description)
                .append(creatorId, op.creatorId)
                .append(writeAccessRoles, op.writeAccessRoles)
                .append(parameters, op.parameters)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(71, 3)
                .append(name)
                .append(view)
                .append(description)
                .append(creatorId)
                .append(writeAccessRoles)
                .append(parameters)
                .hashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("name", name)
                .append("view", view)
                .append("description", description)
                .append("creatorId", creatorId)
                .append("writeAccessRoles", writeAccessRoles)
                .append("parameters", parameters)
                .toString();
    }


    private String buildParamNameString(final String paramKey) {
        return "\"${" + paramKey + "}\"";
    }

    private boolean hasWriteAccess(final String userId, final Set<String> opAuths, final List<String> roles, final String adminAuth) {
        if (null != roles) {
            for (final String role : roles) {
                if (opAuths.contains(role)) {
                    return true;
                }
            }
        }
        if (StringUtils.isNotBlank(adminAuth)) {
            if (opAuths.contains(adminAuth)) {
                return true;
            }
        }
        return null == creatorId || userId.equals(creatorId);
    }

    public static final class Builder {
        private String name;
        private String view;
        private String description;
        private String creatorId;
        private List<String> writers = new ArrayList<>();
        private Map<String, ViewParameterDetail> parameters;

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder view(final String view) {
            if (null != view) {
                this.view = view;
                return this;
            } else {
                throw new IllegalArgumentException("View cannot be null");
            }
        }

        public Builder view(final View view) {
            if (null != view) {
                try {
                    this.view = new String(JSONSerialiser.serialise(view), Charset.forName(CHARSET_NAME));
                    return this;
                } catch (final SerialisationException se) {
                    throw new IllegalArgumentException(se.getMessage());
                }
            } else {
                throw new IllegalArgumentException("View cannot be null");
            }
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        public Builder creatorId(final String creatorId) {
            this.creatorId = creatorId;
            return this;
        }

        public Builder writers(final List<String> writers) {
            this.writers = writers;
            return this;
        }

        public Builder parameters(final Map<String, ViewParameterDetail> parameters) {
            this.parameters = parameters;
            return this;
        }

        public NamedViewDetail build() {
            return new NamedViewDetail(name, view, description, creatorId, writers, parameters);
        }
    }
}
