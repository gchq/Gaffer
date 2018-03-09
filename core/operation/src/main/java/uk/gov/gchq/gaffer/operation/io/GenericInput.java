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
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

public abstract class GenericInput<I> implements Input<I> {
    private I input;

    // This tricks jackson into using both Object input and Object[] input for serialising
    @JsonUnwrapped
    private InputArrayWrapper inputArrayWrapper = new InputArrayWrapper();

    public I getInput() {
        return input;
    }

    public void setInput(final I input) {
        this.input = input;
        updateInputArrayWrapper();
    }

    public void updateInputArrayWrapper() {
        if (null == this.inputArrayWrapper) {
            this.inputArrayWrapper = new InputArrayWrapper();
        }
        this.inputArrayWrapper.setInput(input);
    }

    // -------- JSON getters/setters --------

    @JsonTypeInfo(use = Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter("input")
    Object getInputForJson() {
        if (null == input || input instanceof Object[] || input instanceof Iterable) {
            return null;
        }
        return input;
    }

    @JsonTypeInfo(use = Id.NONE)
    @JsonSetter("input")
    void setInputForJson(final I input) throws SerialisationException {
        final byte[] wrapperJson = JSONSerialiser.serialise(new InputWrapperNoTypeInfo(input));

        // Sometimes json type info is stored in an array of size 2.
        // In that case we cannot determine if the input is multi or not.
        boolean isMulti;

        if (input instanceof Object[]) {
            isMulti = ((Object[]) input).length != 2;
        } else if (input instanceof Iterable) {
            isMulti = Iterables.size((Iterable) input) != 2;
        } else {
            isMulti = false;
        }

        if (isMulti) {
            setInput((I) JSONSerialiser.deserialise(wrapperJson, InputArrayWrapper.class).getInput());
        } else {
            try {
                setInput((I) JSONSerialiser.deserialise(wrapperJson, InputWrapper.class).getInput());
            } catch (final SerialisationException e) {
                // Try assuming it is an array multi
                try {
                    setInput((I) JSONSerialiser.deserialise(wrapperJson, InputArrayWrapper.class).getInput());
                } catch (final SerialisationException e2) {
                    // Just use the original input
                    setInput(input);
                }
            }
        }
    }

    InputArrayWrapper getInputArrayWrapper() {
        return inputArrayWrapper;
    }

    void setInputArrayWrapper(final InputArrayWrapper inputArrayWrapper) {
        this.inputArrayWrapper = inputArrayWrapper;
        updateInputArrayWrapper();
    }

    public static class InputWrapper {
        private Object input;

        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        public Object getInput() {
            return input;
        }

        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        public void setInput(final Object input) {
            this.input = input;
        }
    }

    public static class InputArrayWrapper {
        private Iterable input;

        public Iterable getInput() {
            return input;
        }

        public void setInput(final Object input) {
            if (null == input) {
                this.input = null;
            } else if (input instanceof Object[]) {
                setInput((Object[]) input);
            } else if (input instanceof Iterable) {
                setInput((Iterable) input);
            } else {
                this.input = null;
            }
        }

        public void setInput(final Iterable input) {
            this.input = input;
        }

        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        @JsonSetter("input")
        public void setInput(final Object[] input) {
            if (null == input) {
                this.input = null;
            } else {
                setInput(Lists.newArrayList(input));
            }
        }

        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        @JsonGetter("input")
        Object[] getInputForJson() {
            return Iterables.toArray(input, Object.class);
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
