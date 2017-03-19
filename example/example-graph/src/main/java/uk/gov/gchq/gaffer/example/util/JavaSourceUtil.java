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
package uk.gov.gchq.gaffer.example.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import java.io.File;
import java.io.IOException;

public final class JavaSourceUtil {
    private static final String JAVA_SRC_PATH = "src/main/java/";
    private static final String START_TAG_CODE_SNIPPET_MARKER = String.format("----%n");
    private static final String TAG_END_CODE_SNIPPET_MARKER = "// ----";
    public static final String NEW_LINE = String.format("%n");

    private JavaSourceUtil() {
    }

    public static String getJava(final String className, final String modulePath) {
        return "\n```java\n" + getRawJava(className, modulePath) + "```";
    }

    public static String getRawJava(final String className, final String modulePath) {
        try {
            String path = JAVA_SRC_PATH + className.replace(".", "/") + ".java";
            if (!new File("").getAbsolutePath().endsWith(modulePath)) {
                path = modulePath + "/" + path;
            }
            final String javaCode = FileUtils.readFileToString(new File(path));
            return javaCode.substring(javaCode.indexOf("public class "));
        } catch (final IOException e) {
            throw new RuntimeException("Unable to find the Java source code. This code is used to generate the Wiki and requires access to the Gaffer source code. Please run the code from within the Gaffer parent directory.", e);
        }
    }

    public static String getRawJavaSnippet(final Class<?> clazz, final String modulePath, final String marker, final String start, final String end) {
        String javaCode = getRawJava(clazz.getName(), modulePath);
        final int markerIndex = javaCode.indexOf(marker);
        if (markerIndex > -1) {
            javaCode = javaCode.substring(markerIndex);
            javaCode = javaCode.substring(javaCode.indexOf(start) + start.length());
            javaCode = javaCode.substring(0, javaCode.indexOf(end));
            javaCode = StringUtils.stripEnd(javaCode, " " + NEW_LINE);

            // Remove indentation
            final String trimmedJavaCode = javaCode.trim();
            final int leadingSpaces = javaCode.indexOf(trimmedJavaCode);
            if (leadingSpaces > 0) {
                final String spacesRegex = NEW_LINE + StringUtils.repeat(" ", leadingSpaces);
                javaCode = trimmedJavaCode.replace(spacesRegex, NEW_LINE);
            }
        } else {
            javaCode = "";
        }

        return javaCode;
    }

    public static String getJavaSnippet(final Class<?> clazz, final String modulePath, final String tag) {
        return "\n```java\n" + getRawJavaSnippet(clazz, modulePath, "// [" + tag + "]", START_TAG_CODE_SNIPPET_MARKER, TAG_END_CODE_SNIPPET_MARKER) + String.format("%n```");
    }
}
