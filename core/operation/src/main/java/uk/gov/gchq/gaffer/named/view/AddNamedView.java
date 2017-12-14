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

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewParameterDetail;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

/**
 * A {@code AddNamedView} is an {@link Operation} for adding a {@link NamedView}
 * to a Gaffer graph.
 */
public class AddNamedView implements Operation {
    private static final String CHARSET_NAME = CommonConstants.UTF_8;
    private String name;
    private String namedView;
    private String description;
    private Map<String, ViewParameterDetail> parameters;
    private List<String> mergedNamedViewNames;
    private boolean overwriteFlag = false;
    private Map<String, String> options;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setNamedView(final String namedView) {
        this.namedView = namedView;
    }

    public void setNamedView(final NamedView namedView) {
        this.namedView = new String(namedView.toCompactJson());
    }

    public String getNamedViewAsString() {
        return namedView;
    }

    public NamedView getNamedView() {
        try {
            return JSONSerialiser.deserialise(namedView.getBytes(CHARSET_NAME), NamedView.class);
        } catch (final UnsupportedEncodingException | SerialisationException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public void setParameters(final Map<String, ViewParameterDetail> parameters) {
        this.parameters = parameters;
    }

    public Map<String, ViewParameterDetail> getParameters() {
        return parameters;
    }

    public List<String> getMergedNamedViewNames() {
        return mergedNamedViewNames;
    }

    public void setMergedNamedViewNames(List<String> mergedNamedViewNames) {
        this.mergedNamedViewNames = mergedNamedViewNames;
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

    @Override
    public AddNamedView shallowClone() throws CloneFailedException {
        return new AddNamedView.Builder()
                .name(name)
                .namedView(namedView)
                .description(description)
                .parameters(parameters)
                .mergedNamedViewNames(mergedNamedViewNames)
                .overwrite(overwriteFlag)
                .options(options)
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

        public Builder namedView(final String namedView) {
            _getOp().setNamedView(namedView);
            return _self();
        }

        public Builder namedView(final NamedView namedView) {
            _getOp().setNamedView(namedView);
            return _self();
        }

        public Builder description(final String description) {
            _getOp().setDescription(description);
            return _self();
        }

        public Builder parameters(final Map<String, ViewParameterDetail> parameters) {
            _getOp().setParameters(parameters);
            return _self();
        }

        public Builder mergedNamedViewNames(final List<String> mergedNamedViewNames) {
            _getOp().setMergedNamedViewNames(mergedNamedViewNames);
            return _self();
        }

        public Builder overwrite(final boolean overwriteFlag) {
            _getOp().setOverwriteFlag(overwriteFlag);
            return _self();
        }
    }
}
