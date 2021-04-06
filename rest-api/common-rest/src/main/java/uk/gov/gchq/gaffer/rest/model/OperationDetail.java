/*
 * Copyright 2021 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.serialisation.util.JsonSerialisationUtil;
import uk.gov.gchq.koryphe.Summary;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.serialisation.util.JsonSerialisationUtil.getSerialisedFieldClasses;

public class OperationDetail {
    private final String name;
    private final String summary;
    private final List<OperationField> fields;
    private final Set<Class<? extends Operation>> next;
    private final Operation exampleJson;
    private final String outputClassName;

    public OperationDetail(final Class<? extends Operation> opClass, final Set<Class<? extends Operation>> nextOperations, final Operation exampleJson) {
        this.name = opClass.getName();
        this.summary = getSummaryValue(opClass);
        this.fields = getOperationFields(opClass);
        this.next = nextOperations;
        this.exampleJson = exampleJson;
        this.outputClassName = getOperationOutputType(exampleJson);
    }

    private static List<OperationField> getOperationFields(final Class<? extends Operation> opClass) {
        Map<String, String> fieldsToClassMap = getSerialisedFieldClasses(opClass.getName());
        List<OperationField> operationFields = new ArrayList<>();

        for (final String fieldString : fieldsToClassMap.keySet()) {
            boolean required = false;
            String summary = null;
            Field field = null;
            Set<String> enumOptions = null;

            try {
                field = opClass.getDeclaredField(fieldString);
            } catch (final NoSuchFieldException e) {
                // Ignore, we will just assume it isn't required
            }

            if (null != field) {
                required = null != field.getAnnotation(Required.class);
                summary = getSummaryValue(field.getType());

                if (field.getType().isEnum()) {
                    enumOptions = Stream
                            .of(field.getType().getEnumConstants())
                            .map(Object::toString)
                            .collect(Collectors.toSet());
                }
            }
            operationFields.add(new OperationField(fieldString, summary, fieldsToClassMap.get(fieldString), enumOptions, required));
        }

        return operationFields;
    }

    private static String getOperationOutputType(final Operation operation) {
        String outputClass = null;
        if (operation instanceof Output) {
            outputClass = JsonSerialisationUtil.getTypeString(((Output) operation).getOutputTypeReference().getType());
        }
        return outputClass;
    }

    private static String getSummaryValue(final Class<?> opClass) {
        final Summary summary = opClass.getAnnotation(Summary.class);
        return null != summary && null != summary.value() ? summary.value() : null;
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
