/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore;

import org.apache.hadoop.hbase.TableName;

import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;

/**
 * HBaseProperties contains specific configuration information for the
 * hbase store, such as database connection strings. It wraps
 * {@link uk.gov.gchq.gaffer.data.element.Properties} and lazy loads the all
 * properties from
 * a file when first used.
 */
public class HBaseProperties extends StoreProperties {

}
