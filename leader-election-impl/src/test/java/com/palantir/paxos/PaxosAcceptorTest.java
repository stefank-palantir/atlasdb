/*
 * Copyright 2016 Palantir Technologies
 * ​
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * ​
 * http://opensource.org/licenses/BSD-3-Clause
 * ​
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.paxos;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static junit.framework.TestCase.assertNull;

import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosAcceptorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private PaxosAcceptor acceptor;

    @Before
    public void setUp() throws IOException {
        acceptor = PaxosAcceptorImpl.newAcceptor(folder.newFolder("log").getAbsolutePath());
    }

    @Test
    public void should_accept_first_prepare_request() {
        PaxosProposalId proposalId = new PaxosProposalId(1L, "uuid");
        PaxosPromise promise = acceptor.prepare(1L, proposalId);

        assertThat(promise.ack, is(true));
        assertNull(promise.getLastAcceptedId());
    }

    @Test
    public void should_reject_request_with_lower_promised_id() {
        PaxosProposalId higherProposalId = new PaxosProposalId(2L, "uuid");
        acceptor.prepare(1L, higherProposalId);

        PaxosProposalId proposalId = new PaxosProposalId(1L, "uuid");
        PaxosPromise promise = acceptor.prepare(1L, proposalId);

        assertThat(promise.ack, is(false));
        assertEquals(higherProposalId, promise.promisedId);

    }

}