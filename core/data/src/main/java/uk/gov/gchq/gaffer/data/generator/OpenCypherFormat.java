/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.function.ParseTime;
import uk.gov.gchq.koryphe.impl.function.ToBoolean;
import uk.gov.gchq.koryphe.impl.function.ToDouble;
import uk.gov.gchq.koryphe.impl.function.ToFloat;
import uk.gov.gchq.koryphe.impl.function.ToInteger;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.function.ToString;

import java.util.HashMap;

public abstract class OpenCypherFormat extends CsvFormat {

    public static HashMap<String, KorypheFunction<?, ?>> transformMappings  = new HashMap<String, KorypheFunction<?, ?>>() { {
        put("String", new ToString());
        put("Char", new ToString());
        put("Date", new ToString());
        put("LocalDate", new ToString());
        put("LocalDateTime", new ToString());
        put("Point", new ToString());
        put("Duration", new ToString());
        put("Int", new ToInteger());
        put("Short", new ToInteger());
        put("Byte", new ToInteger());
        put("DateTime", new ParseTime());
        put("Long", new ToLong());
        put("Double", new ToDouble());
        put("Float", new ToFloat());
        put("Boolean", new ToBoolean());
    } };

    public static HashMap<String, String> typeMappings  = new HashMap<String, String>() { {
        put("String", "String");
        put("Character", "Char");
        put("Date", "Date");
        put("LocalDate", "LocalDate");
        put("LocalDateTime", "LocalDateTime");
        put("Point", "Point");
        put("Duration", "Duration");
        put("Integer", "Int");
        put("Short", "Short");
        put("Byte", "Byte");
        put("DateTime", "DateTime");
        put("Long", "Long");
        put("Double", "Double");
        put("Float", "Float");
        put("Boolean", "Boolean");
    } };

}

