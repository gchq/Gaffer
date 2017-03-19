/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.impl.datasketches.theta;

import com.yahoo.sketches.theta.Union;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.ConversionException;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.Converter;

/**
 * A {@link Converter} that converts a {@link Union} into a <code>double</code> so that it can be
 * included in a Dataframe.
 */
public class UnionConverter implements Converter {
    private static final long serialVersionUID = -7267397762494567236L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Union.class.equals(clazz);
    }

    @Override
    public DataType convertedType() {
        return DataTypes.DoubleType;
    }

    @Override
    public Double convert(final Object object) throws ConversionException {
        return ((Union) object).getResult().getEstimate();
    }
}
