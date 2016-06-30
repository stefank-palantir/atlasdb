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
import java.util.concurrent.ConcurrentSkipListMap;

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
    public static final long LOGGED_SEQ = 13L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private PaxosAcceptor acceptor;
    private String logPath;

    @Before
    public void setUp() throws IOException {
        logPath = folder.newFolder("log").getAbsolutePath();
        acceptor = PaxosAcceptorImpl.newAcceptor(logPath);
    }

    // Prepare only
    @Test
    public void should_ack_first_prepare_request() {
        PaxosPromise promise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertThat(promise.ack, is(true));
        assertNull(promise.getLastAcceptedId());
    }

    @Test
    public void should_reject_prepare_request_with_lower_promised_id() {
        acceptor.prepare(SEQ, HIGHER_PROPOSAL_ID);

        PaxosPromise promise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertThat(promise.ack, is(false));
        assertEquals(HIGHER_PROPOSAL_ID, promise.promisedId);
    }

    @Test
    public void should_ack_same_prepare_twice() {
        PaxosPromise expected = PaxosPromise.accept(DEFAULT_PROPOSAL_ID, null, null);

        PaxosPromise firstPromise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);
        PaxosPromise secondPromise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertThat(firstPromise, is(expected));
        assertThat(secondPromise, is(expected));
    }

    // Accept only
    @Test
    public void should_successfully_accept_even_if_never_prepared() {
        BooleanPaxosResponse response = acceptor.accept(SEQ, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(true));
    }

    // Prepare then accept
    @Test
    public void should_successfully_accept_after_prepare_with_same_id() {
        acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        BooleanPaxosResponse response = acceptor.accept(SEQ, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(true));
    }

    @Test
    public void should_not_accept_after_prepare_with_higher_id() {
        acceptor.prepare(SEQ, HIGHER_PROPOSAL_ID);

        BooleanPaxosResponse response = acceptor.accept(SEQ, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(false));
    }

    // Prepare after accept
    @Test
    public void should_ack_prepare_after_accepting_lower_id() {
        PaxosPromise expected = PaxosPromise.accept(HIGHER_PROPOSAL_ID, DEFAULT_PROPOSAL_ID, DEFAULT_VALUE);

        acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);
        acceptor.accept(SEQ, DEFAULT_PROPOSAL);

        PaxosPromise promise = acceptor.prepare(SEQ, HIGHER_PROPOSAL_ID);
        assertEquals(expected, promise);
        assertThat(promise.ack, is(true));
    }

    @Test
    public void should_ack_prepare_after_accepting_same_id() {
        acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);
        acceptor.accept(SEQ, DEFAULT_PROPOSAL);
        PaxosPromise expected = PaxosPromise.accept(DEFAULT_PROPOSAL_ID, DEFAULT_PROPOSAL_ID, DEFAULT_PROPOSAL.getValue());

        PaxosPromise promise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertEquals(expected, promise);
    }

    @Test
    public void should_reject_prepare_after_accepting_higher_id() {
        PaxosPromise expected = PaxosPromise.reject(HIGHER_PROPOSAL_ID);

        acceptor.prepare(SEQ, HIGHER_PROPOSAL_ID);

        // Should the round in the PaxosValue match that in the ProposalId? If so, then this should trigger a failure.
        PaxosProposal higherProposal = new PaxosProposal(HIGHER_PROPOSAL_ID, DEFAULT_VALUE);
        acceptor.accept(SEQ, higherProposal);

        PaxosPromise promise = acceptor.prepare(SEQ, DEFAULT_PROPOSAL_ID);

        assertEquals(expected, promise);
        assertThat(promise.ack, is(false));
    }

    // Tests handling persistence / logs
    // TODO some functionality + tests about logs might belong in PaxosLeaderElectionService
    @Test
    public void should_get_latest_sequence_from_log_before_prepare_or_accept() throws IOException {
        PaxosAcceptorImpl acceptorImpl = getPaxosAcceptorWithPreparedLog();

        long latest = acceptorImpl.getLatestSequencePreparedOrAccepted();

        assertEquals(LOGGED_SEQ, latest);
    }

    @Test
    public void should_get_latest_sequence_from_state_after_prepare_or_accept() {
        PaxosAcceptorImpl acceptorImpl = getPaxosAcceptorWithPreparedLog();
        long newSeq = LOGGED_SEQ + 1;
        acceptorImpl.prepare(newSeq, DEFAULT_PROPOSAL_ID);

        long latest = acceptorImpl.getLatestSequencePreparedOrAccepted();

        assertEquals(newSeq, latest);
        assertEquals(newSeq, acceptorImpl.log.getGreatestLogEntry()); // we should also update the log in this case
    }

    @Test
    public void should_reject_prepare_below_log_cutoff() {
        PaxosAcceptorImpl acceptorImpl = getPaxosAcceptorWithPreparedLog();
        acceptorImpl.log.truncate(LOGGED_SEQ);
        PaxosPromise expected = PaxosPromise.reject(DEFAULT_PROPOSAL_ID);

        PaxosPromise promise = acceptorImpl.prepare(SEQ, DEFAULT_PROPOSAL_ID);
        assertEquals(expected, promise);
    }

    @Test
    public void should_not_accept_below_log_cutoff() {
        PaxosAcceptorImpl acceptorImpl = getPaxosAcceptorWithPreparedLog();
        acceptorImpl.log.truncate(LOGGED_SEQ);

        BooleanPaxosResponse response = acceptorImpl.accept(SEQ, DEFAULT_PROPOSAL);

        assertThat(response.isSuccessful(), is(false));
    }

    @Test
    public void should_reject_prepare_below_previously_logged_entry() {
        PaxosAcceptorImpl acceptorImpl = getPaxosAcceptorWithPreparedLog();
        PaxosPromise expected = PaxosPromise.reject(HIGHER_PROPOSAL_ID);

        PaxosPromise promise = acceptorImpl.prepare(LOGGED_SEQ, DEFAULT_PROPOSAL_ID);

        assertEquals(expected, promise);
    }

    private PaxosAcceptorImpl getPaxosAcceptorWithPreparedLog() {
        PaxosStateLogImpl<PaxosAcceptorState> stateLog = new PaxosStateLogImpl<>(logPath);

        // Prepare the log
        stateLog.writeRound(LOGGED_SEQ, PaxosAcceptorState.newState(HIGHER_PROPOSAL_ID));

        return new PaxosAcceptorImpl(
                new ConcurrentSkipListMap<>(),
                stateLog);
    }
}
