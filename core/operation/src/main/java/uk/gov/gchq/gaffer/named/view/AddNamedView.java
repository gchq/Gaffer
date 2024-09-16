/*
 * Copyright 2017-2023 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewParameterDetail;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@code AddNamedView} is an {@link Operation} for adding a {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView}
 * to a Gaffer graph.
 */
@JsonPropertyOrder(value = {"class", "name", "description", "view"}, alphabetic = true)
@Since("1.3.0")
@Summary("Adds a new named view")
public class AddNamedView implements Operation {
    @Required
    private String name;
    @Required
    private String view;

    private String description;
    private List<String> writeAccessRoles;
    private Map<String, ViewParameterDetail> parameters;
    private boolean overwriteFlag = false;
    private Map<String, String> options;
    private AccessPredicate readAccessPredicate;
    private AccessPredicate writeAccessPredicate;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setView(final String view) {
        this.view = view;
    }

    @JsonSetter("view")
    public void setView(final JsonNode viewNode) {
        this.view = null == viewNode ? null : viewNode.toString();
    }

    @JsonIgnore
    public String getViewAsString() {
        return view;
    }

    @JsonGetter("view")
    public JsonNode getViewAsJsonNode() {
        try {
            return null == view ? null : JSONSerialiser.getJsonNodeFromString(view);
        } catch (final SerialisationException se) {
            throw new IllegalArgumentException(se.getMessage(), se);
        }
    }

    @JsonIgnore
    public void setView(final View view) {
        try {
            this.view = null == view ? null : new String(JSONSerialiser.serialise(view), StandardCharsets.UTF_8);
        } catch (final SerialisationException se) {
            throw new IllegalArgumentException(se.getMessage(), se);
        }
    }

    @JsonIgnore
    public View getView() {
        try {
            return null == view ? null : JSONSerialiser.deserialise(view.getBytes(StandardCharsets.UTF_8), View.class);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @JsonInclude(Include.NON_EMPTY)
    public List<String> getWriteAccessRoles() {
        return writeAccessRoles;
    }

    public void setWriteAccessRoles(final List<String> writeAccessRoles) {
        this.writeAccessRoles = writeAccessRoles;
    }

    public void setParameters(final Map<String, ViewParameterDetail> parameters) {
        this.parameters = parameters;
    }

    public Map<String, ViewParameterDetail> getParameters() {
        return parameters;
    }

    public void setOverwriteFlag(final boolean overwriteFlag) {
        this.overwriteFlag = overwriteFlag;
    }

    public boolean isOverwriteFlag() {
        return overwriteFlag;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    public AccessPredicate getWriteAccessPredicate() {
        return writeAccessPredicate;
    }

    public void setWriteAccessPredicate(final AccessPredicate writeAccessPredicate) {
        this.writeAccessPredicate = writeAccessPredicate;
    }

    public AccessPredicate getReadAccessPredicate() {
        return readAccessPredicate;
    }

    public void setReadAccessPredicate(final AccessPredicate readAccessPredicate) {
        this.readAccessPredicate = readAccessPredicate;
    }

    @Override
    public AddNamedView shallowClone() throws CloneFailedException {
        return new AddNamedView.Builder()
                .name(name)
                .view(view)
                .description(description)
                .writeAccessRoles(writeAccessRoles != null ? writeAccessRoles.toArray(new String[writeAccessRoles.size()]) : null)
                .parameters(parameters)
                .overwrite(overwriteFlag)
                .options(options)
                .readAccessPredicate(readAccessPredicate)
                .writeAccessPredicate(writeAccessPredicate)
                .build();
    }

    public static class Builder extends BaseBuilder<AddNamedView, Builder> {
        public Builder() {
            super(new AddNamedView());
        }

        public Builder name(final String name) {
            _getOp().setName(name);
            return _self();
        }

        public Builder view(final String view) {
            _getOp().setView(view);
            return _self();
        }

        public Builder view(final View view) {
            _getOp().setView(view);
            return _self();
        }

        public Builder description(final String description) {
            _getOp().setDescription(description);
            return _self();
        }

        public Builder writeAccessRoles(final String... roles) {
            if (roles != null) {
                if (_getOp().getWriteAccessRoles() == null) {
                    _getOp().setWriteAccessRoles(new ArrayList<>());
                }
                Collections.addAll(_getOp().getWriteAccessRoles(), roles);
            }
            return _self();
        }

        public Builder parameters(final Map<String, ViewParameterDetail> parameters) {
            _getOp().setParameters(parameters);
            return _self();
        }

        public Builder overwrite(final boolean overwriteFlag) {
            _getOp().setOverwriteFlag(overwriteFlag);
            return _self();
        }

        public Builder readAccessPredicate(final AccessPredicate readAccessPredicate) {
            _getOp().setReadAccessPredicate(readAccessPredicate);
            return _self();
        }

        public Builder writeAccessPredicate(final AccessPredicate writeAccessPredicate) {
            _getOp().setWriteAccessPredicate(writeAccessPredicate);
            return _self();
        }
    }
}
