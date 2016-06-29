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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import static junit.framework.TestCase.assertNull;

import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosAcceptorTest {
    private static final PaxosProposalId DEFAULT_PROPOSAL_ID = new PaxosProposalId(1L, "uuid");
    private static final PaxosValue DEFAULT_VALUE = new PaxosValue("leader_uuid", 1L, null);
    private static final PaxosProposal DEFAULT_PROPOSAL = new PaxosProposal(DEFAULT_PROPOSAL_ID, DEFAULT_VALUE);
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private PaxosAcceptor acceptor;

    @Before
    public void setUp() throws IOException {
        acceptor = PaxosAcceptorImpl.newAcceptor(folder.newFolder("log").getAbsolutePath());
    }

    // Prepare only
    @Test
    public void should_accept_first_prepare_request() {
        PaxosPromise promise = acceptor.prepare(1L, DEFAULT_PROPOSAL_ID);

        assertThat(promise.ack, is(true));
        assertNull(promise.getLastAcceptedId());
    }

    @Test
    public void should_reject_request_with_lower_promised_id() {
        PaxosProposalId higherProposalId = new PaxosProposalId(2L, "uuid");
        acceptor.prepare(1L, higherProposalId);

        PaxosPromise promise = acceptor.prepare(1L, DEFAULT_PROPOSAL_ID);

        assertThat(promise.ack, is(false));
        assertEquals(higherProposalId, promise.promisedId);
    }

    @Test
    public void should_accept_same_propose_twice() {
        PaxosPromise firstPromise = acceptor.prepare(1L, DEFAULT_PROPOSAL_ID);
        PaxosPromise secondPromise = acceptor.prepare(1L, DEFAULT_PROPOSAL_ID);

        assertEquals(firstPromise, secondPromise);
    }

    // Accept only
    @Test
    public void should_accept_if_never_proposed() {
        BooleanPaxosResponse response = acceptor.accept(1L, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(true));
    }

    // Prepare then accept
    @Test
    public void should_accept_after_propose_with_same_id() {
        acceptor.prepare(1L, DEFAULT_PROPOSAL_ID);

        BooleanPaxosResponse response = acceptor.accept(1L, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(true));
    }

    @Test
    public void should_reject_after_propose_with_higher_id() {
        PaxosProposalId higherProposalId = new PaxosProposalId(2L, "uuid");
        acceptor.prepare(1L, higherProposalId);

        BooleanPaxosResponse response = acceptor.accept(1L, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(false));
    }
}