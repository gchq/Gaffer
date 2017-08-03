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
package uk.gov.gchq.gaffer.parquetstore.io.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.types.StructType;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import java.io.IOException;

/**
 * This class is the Parquet writer that will write out {@link Element}'s to a specific Parquet file.
 */
public class ParquetElementWriter extends ParquetWriter<Element> {

    public static Builder builder(final Path file) {
        return new Builder(file);
    }

    @Deprecated
    ParquetElementWriter(final Path file, final WriteSupport<Element> writeSupport,
                         final CompressionCodecName compressionCodecName,
                         final int blockSize, final int pageSize, final boolean enableDictionary,
                         final boolean enableValidation,
                         final ParquetProperties.WriterVersion writerVersion,
                         final Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    public static class Builder extends ParquetWriter.Builder<Element, Builder> {
        private MessageType type = null;
        private boolean isEntity = true;
        private GafferGroupObjectConverter converter = null;
        private StructType sparkSchema = null;

        public Builder(final Path file) {
            super(file);
        }

        public Builder withType(final MessageType type) {
            this.type = type;
            return this;
        }

        public Builder isEntity(final boolean isEntity) {
            this.isEntity = isEntity;
            return this;
        }

        public Builder usingConverter(final GafferGroupObjectConverter converter) {
            this.converter = converter;
            return this;
        }

        public Builder withSparkSchema(final StructType sparkSchema) {
            this.sparkSchema = sparkSchema;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<Element> getWriteSupport(final Configuration conf) {
            return new ElementWriteSupport(type, isEntity, converter, sparkSchema);
        }
    }
}
