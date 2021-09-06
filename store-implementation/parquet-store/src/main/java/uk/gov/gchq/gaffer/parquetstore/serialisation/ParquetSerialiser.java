/*
 * Copyright 2017-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.serialisation;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;

/**
 *  Allows for a custom Parquet schema to be passed to the Parquet store so it can store data in Parquet native types
 *  rather then having everything stored as a {@link byte[]}.
 *
 * @param <INPUT> the de-serialised object
 */
public interface ParquetSerialiser<INPUT> extends Serialiser<INPUT, Object[]> {

    /**
     * This method provides the user a way of specifying the Parquet schema for this object. Note that the
     * root of this schema must have the same name as the input colName.
     *
     * @param colName The column name as a String as seen in the Gaffer schema that this object is from
     * @return A String representation of the part of the Parquet schema that this object will be stored as
     */
    String getParquetSchema(final String colName);

    /**
     * This method provides the user a way of specifying how to convert a Plain Old Java Object (POJO)
     * into the Parquet primitive types.
     *
     * @param object The POJO that you will convert to Parquet primitives
     * @return An object array of Parquet primitives, if this serialiser is used as the vertex serialiser
     * then the order of the objects will determine the sorting order
     * @throws SerialisationException If the POJO fails to be converted to ParquetObjects then this will be thrown
     */
    @Override
    Object[] serialise(final INPUT object) throws SerialisationException;

    /**
     * This method provides the user a way of specifying how to recreate the Plain Old Java Object (POJO)
     * from the Parquet primitive types.
     *
     * @param objects An object array of Parquet primitives
     * @return The POJO that you have recreated from the Parquet primitives
     * @throws SerialisationException If the ParquetObjects fails to be converted to a POJO then this will be thrown
     */
    @Override
    INPUT deserialise(final Object[] objects) throws SerialisationException;
}
