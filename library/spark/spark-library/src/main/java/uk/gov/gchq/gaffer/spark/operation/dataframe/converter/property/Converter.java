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
package uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property;

import org.apache.spark.sql.types.DataType;
import java.io.Serializable;

/**
 * An instance of this interface is used to convert an instance of a particular class into an object who's
 * class matches a particular Spark {@link DataType}.
 */
public interface Converter extends Serializable {

    /**
     * Enables checking whether the converter can convert a particular class.
     *
     * @param clazz the class of the object to convert
     * @return boolean true if it can be handled
     */
    boolean canHandle(final Class clazz);

    /**
     * Returns the {@link DataType} indicating the type of the object that results from the conversion.
     *
     * @return DataType the {@link DataType} corresponding to the class that the object will be converted to.
     */
    DataType convertedType();

    /**
     * Request that the Serialisation serialises some object and returns the raw bytes of the serialised form.
     *
     * @param object the object to be serialised
     * @return Object the converted object
     * @throws ConversionException if the conversion of the object fails
     */
    Object convert(final Object object) throws ConversionException;

}
