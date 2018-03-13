/*
 * Copyright 2018 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.io;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.Collection;
import java.util.Iterator;

/**
 * A {@code GenericInput} is an {@link Input} operation that has an {@link Object}
 * input type, where the input value could be a single Object or an array of Objects.
 * Having a Object input type causes issues with JSON serialisation of Operations
 * so this class is designed to help with the JSON serialisation.
 * This class should be extended for all operations that implement {@code Input<Object>}.
 */
public abstract class GenericInput implements Input<Object> {
    private Object input;
    private MultiInputWrapper multiInputWrapper;

    @Override
    public Object getInput() {
        return _getInput();
    }

    @Override
    public void setInput(final Object input) {
        getMultiInputWrapper().setInput(input);
        _setInput(input);
    }

    private Object _getInput() {
        return input;
    }

    private void _setInput(final Object input) {
        this.input = input;
    }

    private MultiInputWrapper _getMultiInputWrapper() {
        return multiInputWrapper;
    }

    private void _setMultiInputWrapper(final MultiInputWrapper multiInputWrapper) {
        this.multiInputWrapper = multiInputWrapper;
    }


    // -------- JSON getters/setters --------

    @JsonTypeInfo(use = Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter("input")
    Object _getJsonInput() {
        if (getMultiInputWrapper().hasMultiInput()) {
            return null;
        }
        return _getInput();
    }

    @JsonTypeInfo(use = Id.NONE)
    @JsonSetter("input")
    void _setJsonInput(final Object input) throws SerialisationException {
        // Sometimes json type info is stored in an array of size 2.
        // In that case we cannot determine if the input is multi or not.
        boolean isSingular = true;
        if (input instanceof Object[]) {
            isSingular = ((Object[]) input).length == 2 && ((Object[]) input)[0] instanceof String;
        } else if (input instanceof Collection) {
            isSingular = ((Collection) input).size() == 2 && ((Collection) input).iterator().next() instanceof String;
        } else if (input instanceof Iterable) {
            // A bit messy but it is efficient. Only iterators over enough to decide if the length is 2.
            final Iterator itr = ((Iterable) input).iterator();
            try {
                if (itr.hasNext()) {
                    final Object firstItem = itr.next();
                    if (firstItem instanceof String && itr.hasNext()) {
                        itr.next();
                        isSingular = !itr.hasNext();
                    }
                }
            } finally {
                // Try to close the iterator just in case it is closeable.
                CloseableUtil.close(itr);
            }
        }

        final byte[] wrapperJson = JSONSerialiser.serialise(new InputWrapperNoTypeInfo(input));
        Object resultInput = input;
        if (isSingular) {
            try {
                resultInput = (Object) JSONSerialiser.deserialise(wrapperJson, InputWrapper.class).getInput();
            } catch (final SerialisationException e) {
                // Try assuming it is an multi input
                isSingular = false;
            }
        }
        if (!isSingular) {
            try {
                resultInput = (Object) JSONSerialiser.deserialise(wrapperJson, MultiInputWrapper.class).getInputAsIterable();
            } catch (final SerialisationException e2) {
                // Just use the original input
            }
        }

        setInput(resultInput);
    }

    @JsonUnwrapped
    MultiInputWrapper getMultiInputWrapper() {
        MultiInputWrapper multiInputMapper = _getMultiInputWrapper();
        if (null == multiInputMapper) {
            multiInputMapper = new MultiInputWrapper();
            _setMultiInputWrapper(multiInputMapper);
        }
        return multiInputMapper;
    }

    @JsonUnwrapped
    void setMultiInputWrapper(final MultiInputWrapper multiInputWrapper) {
        final MultiInputWrapper newMapper = null == multiInputWrapper ? new MultiInputWrapper() : multiInputWrapper;
        newMapper.setInput(_getInput());
        _setMultiInputWrapper(newMapper);
    }

    // --------------------------------------

    public static class InputWrapper {
        private Object input;

        @JsonTypeInfo(use = Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        public Object getInput() {
            return input;
        }

        @JsonTypeInfo(use = Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        public void setInput(final Object input) {
            this.input = input;
        }
    }

    public static class MultiInputWrapper {
        private Object[] inputArray;
        private Iterable inputIterable;

        @JsonIgnore
        public boolean hasMultiInput() {
            return null != inputArray || null != inputIterable;
        }

        @JsonTypeInfo(use = Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        @JsonGetter("input")
        public Object[] getInputAsArray() {
            if (null == inputArray && null != inputIterable) {
                inputArray = Iterables.toArray(inputIterable, Object.class);
            }
            return inputArray;
        }

        @JsonIgnore
        public Iterable<?> getInputAsIterable() {
            if (null == inputIterable && null != inputArray) {
                inputIterable = Lists.newArrayList(inputArray);
            }
            return inputIterable;
        }

        @JsonTypeInfo(use = Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        @JsonSetter("input")
        public void setInputFromArray(final Object[] input) {
            this.inputArray = input;
            this.inputIterable = null;
        }

        @JsonIgnore
        public void setInputFromIterable(final Iterable input) {
            this.inputArray = null;
            this.inputIterable = input;
        }

        @JsonIgnore
        public void setInput(final Object input) {
            this.inputArray = null;
            this.inputIterable = null;
            if (null != input) {
                if (input instanceof Object[]) {
                    this.inputArray = ((Object[]) input);
                } else if (input instanceof Iterable) {
                    this.inputIterable = ((Iterable) input);
                }
            }
        }
    }

    public static class InputWrapperNoTypeInfo {
        private Object input;

        public InputWrapperNoTypeInfo() {
        }

        public InputWrapperNoTypeInfo(final Object input) {
            this.input = input;
        }

        @JsonTypeInfo(use = Id.NONE)
        public Object getInput() {
            return input;
        }

        @JsonTypeInfo(use = Id.NONE)
        public void setInput(final Object input) {
            this.input = input;
        }
    }
}
