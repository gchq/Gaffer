/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.analytic;

import static java.util.Objects.isNull;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonPropertyOrder(value = {"class", "analyticName", "operationName", "description", "score"}, alphabetic = true)
@Since("1.0.0")
@Summary("Adds a new analytic")
public class AddAnalyticOperation implements Operation {
    @Required
    private String analyticName;
    private String operationName;
    private String description;
    private List<String> readAccessRoles = new ArrayList<>();
    private List<String> writeAccessRoles = new ArrayList<>();
    private boolean overwriteFlag = false;
    private Map<String, UIMappingDetail> uiMapping;
    private Map<String, String> options;
    private Integer score;
    private Map<String, String> metaData;
    private Map<String, String> outputType;

    public boolean isOverwriteFlag() {
        return overwriteFlag;
    }

    public void setOverwriteFlag(final boolean overwriteFlag) {
        this.overwriteFlag = overwriteFlag;
    }

    public void setAnalyticName(final String analyticName) {
        this.analyticName = analyticName;
    }

    public String getAnalyticName() {
        return analyticName;
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(final String operationName) {
        this.operationName = operationName;
    }

    public List<String> getReadAccessRoles() {
        return readAccessRoles;
    }

    public void setReadAccessRoles(final List<String> readAccessRoles) {
        this.readAccessRoles = readAccessRoles;
    }

    public List<String> getWriteAccessRoles() {
        return writeAccessRoles;
    }

    public void setWriteAccessRoles(final List<String> writeAccessRoles) {
        this.writeAccessRoles = writeAccessRoles;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @JsonSetter("uiMapping")
    public void setUiMapping(final Map<String, UIMappingDetail> uiMapping) {
        this.uiMapping = uiMapping;
    }

    public Map<String, UIMappingDetail> getUiMapping() {
        return uiMapping;
    }

    @JsonSetter("metaData")
    public void setMetaData(final Map<String, String> metaData) {
        this.metaData = metaData;
    }

    public Map<String, String> getMetaData() {
        return metaData;
    }

    @JsonSetter("outputType")
    public void setOutputType(final Map<String, String> outputType) {
        this.outputType = outputType;
    }

    public Map<String, String> getOutputType() {
        return outputType;
    }

    @Override
    public AddAnalyticOperation shallowClone() {
        return new AddAnalyticOperation.Builder()
                .analyticName(analyticName)
                .operationName(operationName)
                .description(description)
                .readAccessRoles(readAccessRoles.toArray(new String[readAccessRoles.size()]))
                .writeAccessRoles(writeAccessRoles.toArray(new String[writeAccessRoles.size()]))
                .overwrite(overwriteFlag)
                .uiMapping(uiMapping)
                .metaData(metaData)
                .outputType(outputType)
                .options(options)
                .score(score)
                .build();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final AddAnalyticOperation other = (AddAnalyticOperation) obj;

        return new EqualsBuilder()
                .append(analyticName, other.analyticName)
                .append(operationName, other.operationName)
                .append(description, other.description)
                .append(readAccessRoles, other.readAccessRoles)
                .append(writeAccessRoles, other.writeAccessRoles)
                .append(overwriteFlag, other.overwriteFlag)
                .append(uiMapping, other.uiMapping)
                .append(metaData, other.metaData)
                .append(outputType, other.outputType)
                .append(options, other.options)
                .append(score, other.score)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(67, 23)
                .append(analyticName)
                .append(operationName)
                .append(description)
                .append(readAccessRoles)
                .append(writeAccessRoles)
                .append(overwriteFlag)
                .append(uiMapping)
                .append(metaData)
                .append(outputType)
                .append(options)
                .append(score)
                .toHashCode();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(final Integer score) {
        this.score = score;
    }

    public static class Builder extends BaseBuilder<AddAnalyticOperation, AddAnalyticOperation.Builder> {
        public Builder() {
            super(new AddAnalyticOperation());
        }

        public AddAnalyticOperation.Builder analyticName(final String analyticName) {
            _getOp().setAnalyticName(analyticName);
            return _self();
        }

        public AddAnalyticOperation.Builder operationName(final String name) {
            _getOp().setOperationName(name);
            return _self();
        }

        public AddAnalyticOperation.Builder description(final String description) {
            _getOp().setDescription(description);
            return _self();
        }

        public AddAnalyticOperation.Builder readAccessRoles(final String... roles) {
            Collections.addAll(_getOp().getReadAccessRoles(), roles);
            return _self();
        }

        public AddAnalyticOperation.Builder writeAccessRoles(final String... roles) {
            Collections.addAll(_getOp().getWriteAccessRoles(), roles);
            return _self();
        }

        public AddAnalyticOperation.Builder uiMapping(final Map<String, UIMappingDetail> uiMapping) {
            _getOp().setUiMapping(uiMapping);
            return _self();
        }

        public AddAnalyticOperation.Builder uiMapping(final String name, final UIMappingDetail detail) {
            Map<String, UIMappingDetail> uiMapping = _getOp().getUiMapping();
            if (isNull(uiMapping)) {
                uiMapping = new HashMap<>();
                _getOp().setUiMapping(uiMapping);
            }
            uiMapping.put(name, detail);
            return _self();
        }

        public AddAnalyticOperation.Builder metaData(final Map<String, String> metaData) {
            _getOp().setMetaData(metaData);
            return _self();
        }

        public AddAnalyticOperation.Builder outputType(final Map<String, String> outputType) {
            _getOp().setOutputType(outputType);
            return _self();
        }

        public AddAnalyticOperation.Builder overwrite(final boolean overwriteFlag) {
            _getOp().setOverwriteFlag(overwriteFlag);
            return _self();
        }

        public AddAnalyticOperation.Builder overwrite() {
            return overwrite(true);
        }

        public AddAnalyticOperation.Builder score(final Integer score) {
            _getOp().setScore(score);
            return _self();
        }
    }

}
