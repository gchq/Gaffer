/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.spark.operation.dataframe;

import org.apache.spark.sql.Row;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import uk.gov.gchq.gaffer.data.element.Element;

/**
 * Constants that provide {@link ClassTag}s of various types.
 */
public final class ClassTagConstants {

    private ClassTagConstants() {

    }

    public static final ClassTag<Element> ELEMENT_CLASS_TAG = ClassTag$.MODULE$.apply(Element.class);
    public static final ClassTag<Row> ROW_CLASS_TAG = ClassTag$.MODULE$.apply(Row.class);
}
