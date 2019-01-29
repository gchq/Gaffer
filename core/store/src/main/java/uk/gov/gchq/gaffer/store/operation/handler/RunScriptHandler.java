/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.RunScript;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.util.RunScriptUsingScriptEngine;
import uk.gov.gchq.gaffer.store.operation.handler.util.ScriptType;

import java.util.LinkedHashSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * A {@code RunScriptHandler} handles for {@link RunScript} operations.
 * This handler only supports javascript. You can extend this handler to implement handling for other script types.
 */
@JsonPropertyOrder(alphabetic = true)
public class RunScriptHandler<I, O> implements OutputOperationHandler<RunScript<I, O>, O> {
    private LinkedHashSet<ScriptType> scriptTypes;

    public RunScriptHandler() {
        this(true);
    }

    public RunScriptHandler(final boolean addDefaultTypes) {
        this.scriptTypes = new LinkedHashSet<>();
        if (addDefaultTypes) {
            addDefaultTypes();
        }
    }

    @Override
    public O doOperation(final RunScript<I, O> operation, final Context context, final Store store) throws OperationException {
        String type = operation.getType();
        BiFunction<Object, String, Object> handler = null;

        for (final ScriptType scriptType : scriptTypes) {
            if (nonNull(scriptType.getRegex()) && scriptType.getRegex().matcher(type).matches()) {
                handler = scriptType.getHandler();
                break;
            }
        }

        if (isNull(handler)) {
            throw new IllegalArgumentException("Script type " + type + " is not allowed. These are the allowed patterns: " + scriptTypes.stream().map(ScriptType::getRegexString).collect(Collectors.joining(", ")));
        }

        final Object result = handler.apply(operation.getInput(), operation.getScript());

        try {
            return (O) result;
        } catch (final ClassCastException e) {
            throw new RuntimeException(
                    "The script return the wrong type, it should have been "
                            + operation.getOutputClass().getName()
                            + " but it was "
                            + (nonNull(result) ? result.getClass().getName() : "null"));
        }
    }

    protected void addType(final String regex, final BiFunction<Object, String, Object> handler) {
        scriptTypes.add(new ScriptType(regex, handler));
    }

    private void addDefaultTypes() {
        addType("(?i)(javascript)", new RunScriptUsingScriptEngine("JavaScript"));
        addType("(?i)(js)", new RunScriptUsingScriptEngine("JavaScript"));
    }

    public LinkedHashSet<ScriptType> getScriptTypes() {
        return scriptTypes;
    }

    public void setScriptTypes(final LinkedHashSet<ScriptType> scriptTypes) {
        if (isNull(scriptTypes)) {
            this.scriptTypes = new LinkedHashSet<>();
        } else {
            this.scriptTypes = scriptTypes;
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RunScriptHandler<?, ?> that = (RunScriptHandler<?, ?>) o;

        return new EqualsBuilder()
                .append(scriptTypes, that.scriptTypes)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(7, 37)
                .append(scriptTypes)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("scriptTypes", scriptTypes)
                .toString();
    }
}
