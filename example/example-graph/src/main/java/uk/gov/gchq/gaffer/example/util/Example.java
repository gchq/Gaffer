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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.io.UnsupportedEncodingException;
import java.util.Locale;

public abstract class Example {

    public static final String CAPITALS_AND_NUMBERS_REGEX = "((?=[A-Z])|(?<=[0-9])(?=[a-zA-Z])|(?<=[a-zA-Z])(?=[0-9]))";
    public static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    public static final String DIVIDER = "-----------------------------------------------";
    public static final String TITLE_DIVIDER = DIVIDER;
    public static final String METHOD_DIVIDER = DIVIDER + "\n";
    private static final String JAVA_DOC_URL_PREFIX = "http://gchq.github.io/Gaffer/";
    private final Class<?> classForExample;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String description;

    public Example(final Class<?> classForExample) {
        this(classForExample, "");
    }

    public Example(final Class<?> classForExample, final String description) {
        this.classForExample = classForExample;
        this.description = description;
    }

    public void run() {
        log(classForExample.getSimpleName() + " example");
        log(TITLE_DIVIDER);
        log("See javadoc - [" + classForExample.getName() + "](" + JAVA_DOC_URL_PREFIX + classForExample.getName().replace(".", "/") + ".html).\n");
        printDescription();

        runExamples();
    }

    public Class<?> getClassForExample() {
        return classForExample;
    }

    protected abstract void runExamples();

    protected String getMethodName(final int parentMethod) {
        return Thread.currentThread().getStackTrace()[parentMethod + 2].getMethodName();
    }

    protected String getMethodNameAsSentence(final int parentMethod) {
        final String[] words = getMethodName(parentMethod + 1).split(CAPITALS_AND_NUMBERS_REGEX);
        final StringBuilder sentence = new StringBuilder();
        for (final String word : words) {
            sentence.append(word.toLowerCase(Locale.getDefault()))
                    .append(" ");
        }
        sentence.replace(0, 1, sentence.substring(0, 1).toUpperCase(Locale.getDefault()));
        sentence.replace(sentence.length() - 1, sentence.length(), "");
        return sentence.toString();
    }

    protected void printDescription() {
        if (StringUtils.isNotEmpty(description)) {
            log(description);
        }
    }

    protected void printJava(final String java) {
        log("As Java:");
        log("\n\n```java");
        log(java);
        log("```\n");
    }

    protected void printScala(final String scala) {
        log("As Scala:");
        log("\n\n```scala");
        log(scala);
        log("```\n");
    }

    protected void printJson(final String json) {
        log("As JSON:");
        log("\n\n```json");
        log(json);
        log("```\n");
    }

    protected void printAsJson(final Object object) {
        printJson(getJson(object));
    }

    protected String getJson(final Object object) {
        try {
            return new String(JSON_SERIALISER.serialise(object, true), CommonConstants.UTF_8);
        } catch (final SerialisationException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }


    protected void log(final String message) {
        logger.info(message);
    }
}
