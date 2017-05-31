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
package uk.gov.gchq.gaffer.doc.walkthrough;

import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;

public class AbstractWalkthroughRunner {
    public static final String EXAMPLE_DIVIDER = "\n\n";

    private final List<Class<? extends AbstractWalkthrough>> examples;

    private final String modulePath;
    private final String resourcePrefix;

    public AbstractWalkthroughRunner(final List<Class<? extends AbstractWalkthrough>> examples, final String modulePath, final String resourcePrefix) {
        this.examples = examples;
        this.modulePath = modulePath;
        this.resourcePrefix = resourcePrefix;
    }

    public void run() throws Exception {
        printHeader();
        printTableOfContents();
        printIntro();
        printWalkthroughTitle();
        for (final Class<? extends AbstractWalkthrough> aClass : examples) {
            System.out.println(aClass.newInstance().walkthrough());
            System.out.println(EXAMPLE_DIVIDER);
        }
    }

    private void printIntro() {
        final String intro;
        try (final InputStream stream = StreamUtil.openStream(getClass(), resourcePrefix + "/walkthrough/Intro.md")) {
            intro = new String(IOUtils.toByteArray(stream), CommonConstants.UTF_8);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println(WalkthroughStrSubstitutor.substitute(intro, modulePath));
    }

    private void printHeader() {
        System.out.println("Copyright 2016-2017 Crown Copyright\n"
                + "\n"
                + "Licensed under the Apache License, Version 2.0 (the \"License\");\n"
                + "you may not use this file except in compliance with the License.\n"
                + "You may obtain a copy of the License at\n"
                + "\n"
                + "  http://www.apache.org/licenses/LICENSE-2.0\n"
                + "\n"
                + "Unless required by applicable law or agreed to in writing, software\n"
                + "distributed under the License is distributed on an \"AS IS\" BASIS,\n"
                + "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
                + "See the License for the specific language governing permissions and\n"
                + "limitations under the License.\n"
                + "\n"
                + "_This page has been generated from code. To make any changes please update the walkthrough docs in the [doc](https://github.com/gchq/Gaffer/tree/master/doc/) module, run it and replace the content of this page with the output._\n\n");
    }

    private void printTableOfContents() throws InstantiationException, IllegalAccessException {
        int index = 1;
        System.out.println(index + ". [Introduction](#introduction)");
        index++;
        System.out.println(index + ". [Walkthroughs](#walkthroughs)");

        index = 1;
        for (final Class<? extends AbstractWalkthrough> aClass : examples) {
            final String header = aClass.newInstance().getHeader();
            System.out.println("   " + index + ". [" + header + "](#" + header.toLowerCase(Locale.getDefault()).replace(" ", "-") + ")");
            index++;
        }
        System.out.println("\n");
    }

    private void printWalkthroughTitle() {
        System.out.println("## Walkthroughs");
    }
}
