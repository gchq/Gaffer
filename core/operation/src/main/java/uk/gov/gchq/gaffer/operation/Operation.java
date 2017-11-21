/*
 * Copyright 2016 Crown Copyright
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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.koryphe.ValidationResult;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

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
 * Operation implementations need to implement this Operation interface and any of the following interfaces they wish to make use of:
 * {@link uk.gov.gchq.gaffer.operation.io.Input}
 * {@link uk.gov.gchq.gaffer.operation.io.Output}
 * {@link uk.gov.gchq.gaffer.operation.io.InputOutput} (Use this instead of Input and Output if your operation takes both input and output.)
 * {@link uk.gov.gchq.gaffer.operation.io.MultiInput} (Use this in addition if you operation takes multiple inputs. This will help with json  serialisation)
 * {@link uk.gov.gchq.gaffer.operation.SeedMatching}
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
 *         implements InputOutput.Builder&lt;GetElements, Iterable&lt;? extends ElementId&gt;, CloseableIterable&lt;? extends Element&gt;, Builder&gt;,
 *         MultiInput.Builder&lt;GetElements, ElementId, Builder&gt;,
 *         SeededGraphFilters.Builder&lt;GetElements, Builder&gt;,
 *         SeedMatching.Builder&lt;GetElements, Builder&gt; {
 *     public Builder() {
 *             super(new GetElements());
 *     }
 * }
 * </pre>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface Operation extends Closeable {
    /**
     * Operation implementations should ensure a ShallowClone method is implemented.
     * Performs a shallow clone. Creates a new instance and copies the fields across.
     * It does not clone the fields.
     *
     * If the operation contains nested operations, these must also be cloned.
     *
     * @return shallow clone
     * @throws CloneFailedException if a Clone error occurs
     */
    Operation shallowClone() throws CloneFailedException;

    /**
     * @return the operation options. This may contain store specific options such as authorisation strings or and
     * other properties required for the operation to be executed. Note these options will probably not be interpreted
     * in the same way by every store implementation.
     */
    Map<String, String> getOptions();

    /**
     * @param options the operation options. This may contain store specific options such as authorisation strings or and
     *                other properties required for the operation to be executed. Note these options will probably not be interpreted
     *                in the same way by every store implementation.
     */
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
        if (null == getOptions()) {
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
        if (null == getOptions()) {
            return null;
        }

        return getOptions().get(name);
    }

    /**
     * Gets an operation option by its given name.
     *
     * @param name         the name of the option
     * @param defaultValue the default value to return if value is null.
     * @return the value of the option
     */
    default String getOption(final String name, final String defaultValue) {
        final String rtn;
        if (null == getOptions()) {
            rtn = defaultValue;
        } else {
            rtn = getOptions().get(name);
        }
        return (null == rtn) ? defaultValue : rtn;
    }

    @JsonGetter("options")
    default Map<String, String> _getNullOrOptions() {
        if (null == getOptions()) {
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
        for (final Field field : getClass().getDeclaredFields()) {
            final Required[] annotations = field.getAnnotationsByType(Required.class);
            if (null != annotations && annotations.length > 0) {
                if (field.isAccessible()) {
                    final Object value;
                    try {
                        value = field.get(this);
                    } catch (final IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }

                    if (null == value) {
                        result.addError(field.getName() + " is required for: " + this.getClass().getSimpleName());
                    }
                } else {
                    AccessController.doPrivileged((PrivilegedAction<Operation>) () -> {
                        field.setAccessible(true);
                        final Object value;
                        try {
                            value = field.get(this);
                        } catch (final IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }

                        if (null == value) {
                            result.addError(field.getName() + " is required for: " + this.getClass().getSimpleName());
                        }
                        return null;
                    });
                }
            }
        }

        return result;
    }

    /**
     * This has been replaced with {@link OperationChain#wrap(Operation)}
     *
     * @param operation the operation to wrap into a chain
     * @param <O>       the output type of the operation chain
     * @return the operation chain
     * @deprecated see {@link OperationChain#wrap(Operation)}
     */
    @Deprecated
    static <O> OperationChain<O> asOperationChain(final Operation operation) {
        return (OperationChain<O>) OperationChain.wrap(operation);
    }

    interface Builder<OP, B extends Builder<OP, ?>> {
        OP _getOp();

        B _self();
    }

    abstract class BaseBuilder<OP extends Operation, B extends BaseBuilder<OP, ?>>
            implements Builder<OP, B> {
        private OP op;

        protected BaseBuilder(final OP op) {
            this.op = op;
        }

        /**
         * @param name  the name of the option to add
         * @param value the value of the option to add
         * @return this Builder
         * @see Operation#addOption(String, String)
         */
        public B option(final String name, final String value) {
            _getOp().addOption(name, value);
            return _self();
        }

        public B options(final Map<String, String> options) {
            if (null != options) {
                if (null == _getOp().getOptions()) {
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

        @Override
        public B _self() {
            return (B) this;
        }

    }
}

