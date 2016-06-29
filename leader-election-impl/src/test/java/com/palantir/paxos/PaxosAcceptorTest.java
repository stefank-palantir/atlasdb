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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosAcceptorTest {
    private static final long SEQ = 1L;

    private static final PaxosProposalId DEFAULT_PROPOSAL_ID = new PaxosProposalId(1L, "uuid");
    private static final PaxosValue DEFAULT_VALUE = new PaxosValue("leader_uuid", 1L, null);
    private static final PaxosProposal DEFAULT_PROPOSAL = new PaxosProposal(DEFAULT_PROPOSAL_ID, DEFAULT_VALUE);
    private static final PaxosProposalId HIGHER_PROPOSAL_ID = new PaxosProposalId(2L, "uuid");

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
        PaxosPromise promise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertThat(promise.ack, is(true));
        assertNull(promise.getLastAcceptedId());
    }

    @Test
    public void should_reject_request_with_lower_promised_id() {
        acceptor.prepare(SEQ, HIGHER_PROPOSAL_ID);

        PaxosPromise promise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertThat(promise.ack, is(false));
        assertEquals(HIGHER_PROPOSAL_ID, promise.promisedId);
    }

    @Test
    public void should_accept_same_propose_twice() {
        PaxosPromise firstPromise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);
        PaxosPromise secondPromise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertEquals(firstPromise, secondPromise);
    }

    // Accept only
    @Test
    public void should_accept_if_never_proposed() {
        BooleanPaxosResponse response = acceptor.accept(SEQ, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(true));
    }

    // Prepare then accept
    @Test
    public void should_accept_after_propose_with_same_id() {
        acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        BooleanPaxosResponse response = acceptor.accept(SEQ, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(true));
    }

    @Test
    public void should_reject_after_propose_with_higher_id() {
        acceptor.prepare(SEQ, HIGHER_PROPOSAL_ID);

        BooleanPaxosResponse response = acceptor.accept(SEQ, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(false));
    }

    // Prepare after accept
    @Test
    public void should_accept_prepare_after_accepting_lower_id() {
        PaxosPromise expected = new PaxosPromise(HIGHER_PROPOSAL_ID, DEFAULT_PROPOSAL_ID, DEFAULT_VALUE);

        acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);
        acceptor.accept(SEQ, DEFAULT_PROPOSAL);

        PaxosPromise promise = acceptor.prepare(SEQ, HIGHER_PROPOSAL_ID);
        assertEquals(expected, promise);
        assertThat(promise.ack, is(true));
    }

    // This seems like a bug to me...
    @Ignore
    @Test
    public void should_reject_prepare_after_accepting_same_id() {
        acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);
        acceptor.accept(SEQ, DEFAULT_PROPOSAL);
        PaxosPromise promise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertThat(promise.ack, is(false));
    }

    @Test
    public void should_reject_prepare_after_accepting_higher_id() {
        PaxosPromise expected = new PaxosPromise(HIGHER_PROPOSAL_ID);

        acceptor.prepare(SEQ, HIGHER_PROPOSAL_ID);

        // Should the round in the PaxosValue match that in the ProposalId? If so, then this should trigger a failure.
        PaxosProposal higherProposal = new PaxosProposal(HIGHER_PROPOSAL_ID, DEFAULT_VALUE);
        acceptor.accept(SEQ, higherProposal);

        PaxosPromise promise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertEquals(expected, promise);
        assertThat(promise.ack, is(false));
    }
}