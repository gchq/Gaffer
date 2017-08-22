/*
 * Copyright 2017. Crown Copyright
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
package uk.gov.gchq.gaffer.parquetstore.io.reader;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import java.io.IOException;

/**
 * This is the Parquet reader that can read the Parquet files directly to Elements provided the files are written the
 * same way that the {@link uk.gov.gchq.gaffer.parquetstore.io.writer.ParquetElementWriter} does.
 */
public class ParquetElementReader extends ParquetReader<Element> {

    public static ParquetElementReader.Builder builder(final Path file) {
        return new ParquetElementReader.Builder(file);
    }

    protected ParquetElementReader(final Path file, final ReadSupport<Element> readSupport) throws IOException {
        super(file, readSupport);
        throw new UnsupportedOperationException("Use the builder to construct the ParquetElementReader.");
    }

    public static class Builder<Element> extends ParquetReader.Builder<Element> {
        private boolean isEntity;
        private GafferGroupObjectConverter converter;

        public Builder(final Path path) {
            super(path);
        }

        public ParquetElementReader.Builder<Element> isEntity(final boolean isEntity) {
            this.isEntity = isEntity;
            return this;
        }

        public ParquetElementReader.Builder<Element> usingConverter(final GafferGroupObjectConverter converter) {
            this.converter = converter;
            return this;
        }

        @Override
        protected ReadSupport<Element> getReadSupport() {
            return (ReadSupport<Element>) new ElementReadSupport(isEntity, converter);
        }
    }
}
