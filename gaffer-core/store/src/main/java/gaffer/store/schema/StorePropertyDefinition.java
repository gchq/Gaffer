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

package gaffer.store.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.serialisation.Serialisation;
import gaffer.serialisation.implementation.JavaSerialiser;

import java.io.Serializable;

/**
 * A <code>StorePropertyDefinition</code> holds information about how properties get serialised into and out of the
 * {@link gaffer.store.Store}.
 *
 * @see gaffer.store.schema.StorePropertyDefinition.Builder
 */
public class StorePropertyDefinition implements Serializable {
    private static final long serialVersionUID = -6545159644647966605L;
    private static final Serialisation DEFAULT_SERIALISER = new JavaSerialiser();

    private Serialisation serialiser = DEFAULT_SERIALISER;
    private String position;

    public StorePropertyDefinition() {
    }

    public StorePropertyDefinition(final Serialisation serialiser, final String position) {
        this.serialiser = serialiser;
        this.position = position;
    }

    /**
     * @return the {@link gaffer.serialisation.Serialisation} for the property. If one has not been explicitly set then
     * it will default to a {@link gaffer.serialisation.implementation.JavaSerialiser}.
     */
    @JsonIgnore
    public Serialisation getSerialiser() {
        return serialiser;
    }

    /**
     * @param serialiser the {@link gaffer.serialisation.Serialisation} for the property. If null then
     *                   a {@link gaffer.serialisation.implementation.JavaSerialiser} will be set instead.
     */
    public void setSerialiser(final Serialisation serialiser) {
        if (null == serialiser) {
            this.serialiser = DEFAULT_SERIALISER;
        } else {
            this.serialiser = serialiser;
        }
    }

    public String getSerialiserClass() {
        final Class<? extends Serialisation> serialiserClass = serialiser.getClass();
        if (!DEFAULT_SERIALISER.getClass().equals(serialiserClass)) {
            return serialiserClass.getName();
        }

        return null;
    }

    public void setSerialiserClass(final String clazz) {
        if (null == clazz) {
            this.serialiser = DEFAULT_SERIALISER;
        } else {
            final Class<? extends Serialisation> serialiserClass;
            try {
                serialiserClass = Class.forName(clazz).asSubclass(Serialisation.class);
            } catch (ClassNotFoundException e) {
                throw new SchemaException(e.getMessage(), e);
            }
            try {
                this.serialiser = serialiserClass.newInstance();
            } catch (IllegalAccessException | IllegalArgumentException | SecurityException | InstantiationException e) {
                throw new SchemaException(e.getMessage(), e);
            }
        }
    }

    /**
     * @return the position to store the property. This can be interpreted differently by different
     * {@link gaffer.store.Store} implementations. For example it could refer to the column to store the property in.
     */
    public String getPosition() {
        return position;
    }

    /**
     * @param position the position to store the property. This can be interpreted differently by different
     *                 {@link gaffer.store.Store} implementations. For example it could refer to the column to store the property in.
     */
    public void setPosition(final String position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "StoreElementComponentDefinition{"
                + "serialiser=" + serialiser
                + ", position=" + position
                + '}';
    }

    /**
     * Builder for {@link gaffer.store.schema.StorePropertyDefinition}.
     *
     * @see gaffer.store.schema.StorePropertyDefinition
     */
    public static class Builder {
        private final StorePropertyDefinition propertyDef;

        public Builder() {
            this(new StorePropertyDefinition());
        }

        public Builder(final StorePropertyDefinition propertyDef) {
            this.propertyDef = propertyDef;
        }

        /**
         * @param serialiser the {@link gaffer.serialisation.Serialisation} to set.
         * @return this Builder
         * @see gaffer.store.schema.StorePropertyDefinition#setSerialiser(gaffer.serialisation.Serialisation)
         */
        public Builder serialiser(final Serialisation serialiser) {
            propertyDef.setSerialiser(serialiser);
            return this;
        }

        /**
         * @param serialiser the {@link gaffer.serialisation.Serialisation} class name to set.
         * @return this Builder
         * @see gaffer.store.schema.StorePropertyDefinition#setSerialiserClass(java.lang.String)
         */
        public Builder serialiser(final String serialiser) {
            propertyDef.setSerialiserClass(serialiser);
            return this;
        }

        /**
         * @param position the position to set.
         * @return this Builder
         * @see gaffer.store.schema.StorePropertyDefinition#setPosition(java.lang.String)
         */
        public Builder position(final String position) {
            propertyDef.setPosition(position);
            return this;
        }

        /**
         * Builds the {@link gaffer.store.schema.StorePropertyDefinition}.
         *
         * @return the built {@link gaffer.store.schema.StorePropertyDefinition}
         */
        public StorePropertyDefinition build() {
            return propertyDef;
        }
    }
}
