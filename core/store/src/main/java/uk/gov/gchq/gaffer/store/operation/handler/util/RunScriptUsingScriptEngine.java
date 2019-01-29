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
package uk.gov.gchq.gaffer.store.operation.handler.util;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.util.function.BiFunction;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/**
 * Runs a script using the {@link ScriptEngine}.
 */
public class RunScriptUsingScriptEngine implements BiFunction<Object, String, Object> {
    private String type;

    public RunScriptUsingScriptEngine() {
    }

    public RunScriptUsingScriptEngine(final String type) {
        this.type = type;
    }

    @Override
    public Object apply(final Object input, final String script) {
        requireNonNull(type, "The script engine type has not been configured");

        final ScriptEngineManager factory = new ScriptEngineManager();
        final ScriptEngine engine = factory.getEngineByName(type);
        final Invocable inv = (Invocable) engine;

        if (isNull(engine)) {
            throw new RuntimeException("Script type: " + type + " was not recognised");
        }

        try {
            engine.eval(script);
        } catch (final ScriptException e) {
            throw new RuntimeException("Failed to evaluate script.", e);
        }

        final Object result;
        try {
            result = inv.invokeFunction("apply", input);
        } catch (final ScriptException e) {
            throw new RuntimeException("Failed to execute apply method on script.", e);
        } catch (final NoSuchMethodException e) {
            throw new RuntimeException("Failed to execute script as method did not exist.", e);
        }

        return result;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RunScriptUsingScriptEngine that = (RunScriptUsingScriptEngine) o;

        return new EqualsBuilder()
                .append(type, that.type)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(type)
                .toHashCode();
    }
}
