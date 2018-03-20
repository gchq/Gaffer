/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.elementvisibilityutil;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This test is coped from org.apache.accumulo.core.security.Authorizations.
 */
public class Authorisations implements Iterable<byte[]>, Serializable {
    private static final long serialVersionUID = 8931467369628123909L;
    private Set<ArrayByteSequence> auths = new HashSet();
    private List<byte[]> authsList = new ArrayList();
    private static final boolean[] VALID_AUTH_CHARS = new boolean[256];
    public static final String HEADER = "!AUTH1:";

    public Authorisations() {
    }

    public Authorisations(final String... authorisations) {
        this.setAuthorisations(authorisations);
    }

    public Authorisations(final byte[] authorisations) {

        checkArgument(authorisations != null, "authorizations is null");

        String authsString = new String(authorisations, UTF_8);
        if (authsString.startsWith(HEADER)) {
            // it's the new format
            authsString = authsString.substring(HEADER.length());
            if (authsString.length() > 0) {
                for (final String encAuth : authsString.split(",")) {
                    byte[] auth = Base64.getDecoder().decode(encAuth);
                    auths.add(new ArrayByteSequence(auth));
                }
                checkAuths();
            }
        } else {
            // it's the old format
            if (authorisations.length > 0) {
                setAuthorisations(authsString.split(","));
            }
        }
    }

    public List<byte[]> getAuthorisations() {
        ArrayList copy = new ArrayList(this.authsList.size());
        Iterator var2 = this.authsList.iterator();

        while (var2.hasNext()) {
            byte[] auth = (byte[]) var2.next();
            byte[] bytes = new byte[auth.length];
            System.arraycopy(auth, 0, bytes, 0, auth.length);
            copy.add(bytes);
        }

        return Collections.unmodifiableList(copy);
    }

    public byte[] getAuthorisationsArray() {
        return serialise().getBytes(UTF_8);
    }

    public List<ByteBuffer> getAuthorisationsBB() {
        ArrayList<ByteBuffer> copy = new ArrayList<>(authsList.size());
        for (final byte[] auth : authsList) {
            byte[] bytes = new byte[auth.length];
            System.arraycopy(auth, 0, bytes, 0, auth.length);
            copy.add(ByteBuffer.wrap(bytes));
        }
        return Collections.unmodifiableList(copy);
    }

    public String serialise() {
        StringBuilder sb = new StringBuilder(HEADER);
        String sep = "";
        for (final byte[] auth : authsList) {
            sb.append(sep);
            sep = ",";
            sb.append(Base64.getEncoder().encodeToString(auth));
        }

        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        Iterator var3 = this.auths.iterator();

        while (var3.hasNext()) {
            ArrayByteSequence auth = (ArrayByteSequence) var3.next();
            sb.append(sep);
            sep = ",";
            sb.append(new String(auth.toArray(), StandardCharsets.UTF_8));
        }

        return sb.toString();
    }

    public boolean contains(final byte[] auth) {
        return this.auths.contains(new ArrayByteSequence(auth));
    }

    public boolean contains(final ArrayByteSequence auth) {
        return this.auths.contains(auth);
    }

    public boolean contains(final String auth) {
        return this.auths.contains(new ArrayByteSequence(auth));
    }

    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        } else if (o instanceof Authorisations) {
            Authorisations ao = (Authorisations) o;
            return this.auths.equals(ao.auths);
        } else {
            return false;
        }
    }

    public int hashCode() {
        int result = 0;

        ArrayByteSequence b;
        for (Iterator var2 = this.auths.iterator(); var2.hasNext(); result += b.hashCode()) {
            b = (ArrayByteSequence) var2.next();
        }

        return result;
    }

    public int size() {
        return this.auths.size();
    }

    public boolean isEmpty() {
        return this.auths.isEmpty();
    }

    public Iterator<byte[]> iterator() {
        return this.getAuthorisations().iterator();
    }

    static final boolean isValidAuthChar(final byte b) {
        return VALID_AUTH_CHARS[255 & b];
    }

    static {
        int i;
        for (i = 0; i < 256; ++i) {
            VALID_AUTH_CHARS[i] = false;
        }

        for (i = 97; i <= 122; ++i) {
            VALID_AUTH_CHARS[i] = true;
        }

        for (i = 65; i <= 90; ++i) {
            VALID_AUTH_CHARS[i] = true;
        }

        for (i = 48; i <= 57; ++i) {
            VALID_AUTH_CHARS[i] = true;
        }

        VALID_AUTH_CHARS[95] = true;
        VALID_AUTH_CHARS[45] = true;
        VALID_AUTH_CHARS[58] = true;
        VALID_AUTH_CHARS[46] = true;
        VALID_AUTH_CHARS[47] = true;
    }

    private void checkAuths() {
        TreeSet sortedAuths = new TreeSet(this.auths);
        Iterator var2 = sortedAuths.iterator();

        while (var2.hasNext()) {
            ArrayByteSequence bs = (ArrayByteSequence) var2.next();
            if (bs.length() == 0) {
                throw new IllegalArgumentException("Empty authorisation");
            }

            this.authsList.add(bs.toArray());
        }

    }

    private void setAuthorisations(final String... authorisations) {
        checkArgument(authorisations != null, "authorisations is null");
        this.auths.clear();
        String[] var2 = authorisations;
        int var3 = authorisations.length;

        for (int var4 = 0; var4 < var3; ++var4) {
            String str = var2[var4];
            str = str.trim();
            this.auths.add(new ArrayByteSequence(str.getBytes(StandardCharsets.UTF_8)));
        }

        this.checkAuths();
    }
}
