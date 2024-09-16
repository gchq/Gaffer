/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.serialisation.json.JsonSimpleClassName;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * An {@code Operation} defines an operation to be processed on a graph.
 * All operations must to implement this interface.
 * Operations should be written to be as generic as possible to allow them to be applied to different graph/stores.
 * NOTE - operations should not contain the operation logic. The logic should be separated out into a operation handler.
 * This will allow you to execute the same operation on different stores with different handlers.
 * <p>
 * Operations must be JSON serialisable in order to make REST API calls.
 * </p>
 * <p>
 * Any fields that are required should be annotated with the {@link Required} annotation.
 * </p>
 * <p>
 * Operation implementations need to implement this Operation interface and any of the following interfaces they wish to
 * make use of:
 * {@link uk.gov.gchq.gaffer.operation.io.Input}
 * {@link uk.gov.gchq.gaffer.operation.io.Output}
 * {@link uk.gov.gchq.gaffer.operation.io.InputOutput} (Use this instead of Input and Output if your operation takes both input and output.)
 * {@link uk.gov.gchq.gaffer.operation.io.MultiInput} (Use this in addition if you operation takes multiple inputs. This will help with json  serialisation)
 * {@link uk.gov.gchq.gaffer.operation.Validatable}
 * {@link uk.gov.gchq.gaffer.operation.graph.OperationView}
 * {@link uk.gov.gchq.gaffer.operation.graph.GraphFilters}
 * {@link uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters}
 * </p>
 * <p>
 * Each Operation implementation should have a corresponding unit test class
 * that extends the OperationTest class.
 * </p>
 * <p>
 * Implementations should override the close method and ensure all closeable fields are closed.
 * </p>
 * <p>
 * All implementations should also have a static inner Builder class that implements
 * the required builders. For example:
 * </p>
 * <pre>
 * public static class Builder extends Operation.BaseBuilder&lt;GetElements, Builder&gt;
 *         implements InputOutput.Builder&lt;GetElements, Iterable&lt;? extends ElementId&gt;, Iterable&lt;? extends Element&gt;, Builder&gt;,
 *         MultiInput.Builder&lt;GetElements, ElementId, Builder&gt;,
 *         SeededGraphFilters.Builder&lt;GetElements, Builder&gt; {
 *     public Builder() {
 *             super(new GetElements());
 *     }
 * }
 * </pre>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = As.PROPERTY, property = "class", defaultImpl = OperationChain.class)
@JsonSimpleClassName(includeSubtypes = true)
public interface Operation extends Closeable {

    /**
     * Operation implementations should ensure a ShallowClone method is implemented. Performs a shallow clone.
     * Creates a new instance and copies the fields across. It does not clone the fields. If the operation
     * contains nested operations, these must also be cloned.
     *
     * @return shallow clone
     * @throws CloneFailedException if a Clone error occurs
     */
    Operation shallowClone() throws CloneFailedException;

    /**
     * @return the operation options. This may contain store specific options such as authorisation strings or and
     *         other properties required for the operation to be executed. Note these options will probably not be
     *         interpreted in the same way by every store implementation.
     */
    @JsonIgnore
    Map<String, String> getOptions();

    /**
     * @param options the operation options. This may contain store specific options such as authorisation strings or
     *                and other properties required for the operation to be executed. Note these options will probably not
     *                be interpreted in the same way by every store implementation.
     */
    @JsonSetter
    void setOptions(final Map<String, String> options);

    /**
     * Adds an operation option. This may contain store specific options such as authorisation strings or and
     * other properties required for the operation to be executed. Note these options will probably not be interpreted
     * in the same way by every store implementation.
     *
     * @param name  the name of the option
     * @param value the value of the option
     */
    default void addOption(final String name, final String value) {
        if (isNull(getOptions())) {
            setOptions(new HashMap<>());
        }

        getOptions().put(name, value);
    }

    /**
     * Gets an operation option by its given name.
     *
     * @param name the name of the option
     * @return the value of the option
     */
    default String getOption(final String name) {
        return isNull(getOptions())
                ? null
                : getOptions().get(name);
    }

    /**
     * Gets an operation option by its given name.
     *
     * @param name         the name of the option
     * @param defaultValue the default value to return if value is null.
     *
     * @return the value of the option
     */
    default String getOption(final String name, final String defaultValue) {
        return (isNull(getOptions()))
                ? defaultValue
                : getOptions().getOrDefault(name, defaultValue);
    }

    /**
     * Gets if an operation contains an option of the given name.
     *
     * @param name         the name of the option
     *
     * @return if the operation contains the option
     */
    default boolean containsOption(final String name) {
        return !isNull(getOptions()) && getOptions().containsKey(name);
    }

    @JsonGetter("options")
    default Map<String, String> _getNullOrOptions() {
        if (isNull(getOptions())) {
            return null;
        }

        return getOptions().isEmpty() ? null : getOptions();
    }

    /**
     * Operation implementations should ensure that all closeable fields are closed in this method.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    default void close() throws IOException {
        // do nothing by default
    }

    /**
     * Validates an operation. This should be used to validate that fields have been be configured correctly.
     * By default no validation is applied. Override this method to implement validation.
     *
     * @return validation result.
     */
    default ValidationResult validate() {
        final ValidationResult result = new ValidationResult();

        final HashSet<Field> fields = Sets.<Field>newHashSet();
        Class<?> currentClass = this.getClass();
        while (nonNull(currentClass)) {
            fields.addAll(Arrays.asList(currentClass.getDeclaredFields()));
            currentClass = currentClass.getSuperclass();
        }

        for (final Field field : fields) {
            final Required[] annotations = field.getAnnotationsByType(Required.class);
            if (nonNull(annotations) && ArrayUtils.isNotEmpty(annotations)) {
                if (field.isAccessible()) {
                    validateRequiredFieldPresent(result, field);
                } else {
                    AccessController.doPrivileged((PrivilegedAction<Operation>) () -> {
                        field.setAccessible(true);
                        validateRequiredFieldPresent(result, field);
                        return null;
                    });
                }
            }
        }

        return result;
    }

    default void validateRequiredFieldPresent(final ValidationResult result, final Field field) {
        final Object value;
        try {
            value = field.get(this);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        if (isNull(value)) {
            result.addError(String.format("%s is required for: %s", field.getName(), this.getClass().getSimpleName()));
        }
    }

    interface Builder<OP, B extends Builder<OP, ?>> {
        OP _getOp();

        B _self();
    }

    abstract class BaseBuilder<OP extends Operation, B extends BaseBuilder<OP, ?>> implements Builder<OP, B> {

        private final OP op;

        protected BaseBuilder(final OP op) {
            this.op = op;
        }

        /**
         * @param name  the name of the option to add
         * @param value the value of the option to add
         *
         * @return this Builder
         *
         * @see Operation#addOption(String, String)
         */
        public B option(final String name, final String value) {
            _getOp().addOption(name, value);
            return _self();
        }

        public B options(final Map<String, String> options) {
            if (nonNull(options)) {
                if (isNull(_getOp().getOptions())) {
                    _getOp().setOptions(new HashMap<>(options));
                } else {
                    _getOp().getOptions().putAll(options);
                }
            }
            return _self();
        }

        /**
         * Builds the operation and returns it.
         *
         * @return the built operation.
         */
        public OP build() {
            return _getOp();
        }

        @Override
        public OP _getOp() {
            return op;
        }

        @SuppressWarnings("unchecked")
        @Override
        public B _self() {
            return (B) this;
        }
    }
}
