/*
 * Copyright 2023 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.kinesis.multilang.auth;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class KclSTSAssumeRoleSessionCredentialsProviderTest {

    @Test
    public void testVarArgs() {
        for (final String[] varargs : Arrays.asList(
                new String[] { "arn", "session", "externalIdXXXeid", "foo"},
                new String[] { "arn", "session", "foo", "externalIdXXXeid"}
        )) {
            final VarArgsSpy provider = new VarArgsSpy(varargs);
            assertEquals("eid", provider.externalId);
        }
    }

    private static class VarArgsSpy extends KclSTSAssumeRoleSessionCredentialsProvider {

        private String externalId;

        public VarArgsSpy(String[] args) {
            super(args);
        }

        @Override
        public void acceptExternalId(final String externalId) {
            this.externalId = externalId;
            super.acceptExternalId(externalId);
        }
    }
}