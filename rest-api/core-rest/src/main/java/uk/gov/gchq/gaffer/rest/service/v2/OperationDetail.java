/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v2;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.operation.Operation;

import java.util.List;
import java.util.Set;

/**
 * POJO to store details for a user specified {@link uk.gov.gchq.gaffer.operation.Operation}
 * class.
 */
class OperationDetail {
    private OperationServiceV2 operationServiceV2;
    private final String name;
    private final String summary;
    private final List<OperationField> fields;
    private final Set<Class<? extends Operation>> next;
    private final Operation exampleJson;
    private final String outputClassName;

    OperationDetail(final OperationServiceV2 operationServiceV2, final Class<? extends Operation> opClass) {
        this.operationServiceV2 = operationServiceV2;
        this.name = opClass.getName();
        this.summary = OperationServiceV2.getSummaryValue(opClass);
        this.fields = operationServiceV2.getOperationFields(opClass);
        this.next = operationServiceV2.getNextOperations(opClass);
        try {
            this.exampleJson = operationServiceV2.generateExampleJson(opClass);
        } catch (final IllegalAccessException | InstantiationException e) {
            throw new GafferRuntimeException("Could not get operation details for class: " + name, e, Status.BAD_REQUEST);
        }
        this.outputClassName = operationServiceV2.getOperationOutputType(exampleJson);
    }

    public String getName() {
        return name;
    }

    public String getSummary() {
        return summary;
    }

    public List<OperationField> getFields() {
        return fields;
    }

    public Set<Class<? extends Operation>> getNext() {
        return next;
    }

    public Operation getExampleJson() {
        return exampleJson;
    }

    public String getOutputClassName() {
        return outputClassName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final OperationDetail that = (OperationDetail) o;

        return new EqualsBuilder()
                .append(name, that.name)
                .append(summary, that.summary)
                .append(fields, that.fields)
                .append(next, that.next)
                .append(exampleJson, that.exampleJson)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(name)
                .append(summary)
                .append(fields)
                .append(next)
                .append(exampleJson)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("summary", summary)
                .append("fields", fields)
                .append("next", next)
                .append("exampleJson", exampleJson)
                .toString();
    }
}
