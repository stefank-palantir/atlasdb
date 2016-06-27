/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.paxos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class PaxosProposerTest {
    private static final PaxosProposalId PROPOSAL_ID = new PaxosProposalId(1, UUID.randomUUID().toString());
    private static final List<PaxosLearner> NO_LEARNERS = ImmutableList.of();
    private static final BooleanPaxosResponse SUCCESSFUL_ACCEPTANCE = new BooleanPaxosResponse(true);
    private static final BooleanPaxosResponse FAILED_ACCEPTANCE = new BooleanPaxosResponse(false);

    private static final int KEY = 1;
    private static final byte[] VALUE = "hello".getBytes();

    @Mock
    private PaxosLearner learner;
    @Mock
    private PaxosLearner otherLearner;
    @Mock
    private List<PaxosAcceptor> acceptor;
    @Mock
    private List<PaxosLearner> learners;
    @Mock
    private PaxosAcceptor acceptingAcceptor;
    @Mock
    private PaxosAcceptor rejectingAcceptor;
    @Mock
    private PaxosAcceptor promiseThenRejectAcceptor;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    PaxosProposer proposer;

    @Before
    public void setup() {
        when(acceptingAcceptor.prepare(Matchers.anyLong(), any(PaxosProposalId.class))).thenReturn(successfulPromise());
        when(acceptingAcceptor.accept(Matchers.anyLong(), any(PaxosProposal.class))).thenReturn(SUCCESSFUL_ACCEPTANCE);

        when(rejectingAcceptor.prepare(Matchers.anyLong(), any(PaxosProposalId.class))).thenReturn(failedPromise());
        when(rejectingAcceptor.accept(Matchers.anyLong(), any(PaxosProposal.class))).thenReturn(FAILED_ACCEPTANCE);

        when(promiseThenRejectAcceptor.prepare(Matchers.anyLong(), any(PaxosProposalId.class))).thenReturn(successfulPromise());
        when(promiseThenRejectAcceptor.accept(Matchers.anyLong(), any(PaxosProposal.class))).thenReturn(FAILED_ACCEPTANCE);
    }

    @Test public void
    should_accept_a_proposal_if_there_is_only_one_acceptor_and_the_acceptor_accepts_the_proposal() throws PaxosRoundFailureException {
        proposer = PaxosProposerImpl.newProposer(learner, ImmutableList.of(acceptingAcceptor), NO_LEARNERS, 1, executor);

        assertThat(proposer.propose(KEY, VALUE), is(VALUE));
    }

    @Test public void
    should_accept_a_proposal_if_there_are_3_acceptors_2_accept_and_1_rejects() throws PaxosRoundFailureException {
        proposer = PaxosProposerImpl.newProposer(learner, ImmutableList.of(acceptingAcceptor, acceptingAcceptor, rejectingAcceptor), NO_LEARNERS, 2, executor);

        assertThat(proposer.propose(KEY, VALUE), is(VALUE));
    }

    @Test public void
    should_reject_a_proposal_if_there_are_3_acceptors_1_accept_and_2_rejects() throws PaxosRoundFailureException {
        exception.expect(PaxosRoundFailureException.class);

        proposer = PaxosProposerImpl.newProposer(learner, ImmutableList.of(acceptingAcceptor, rejectingAcceptor, rejectingAcceptor), NO_LEARNERS, 2, executor);

        proposer.propose(KEY, VALUE);
    }

    @Test public void
    should_reject_a_proposal_if_there_are_3_acceptors_1_accepts_and_2_promises_then_rejects() throws PaxosRoundFailureException {
        exception.expect(PaxosRoundFailureException.class);

        proposer = PaxosProposerImpl.newProposer(learner, ImmutableList.of(acceptingAcceptor, promiseThenRejectAcceptor, rejectingAcceptor), NO_LEARNERS, 2, executor);

        proposer.propose(KEY, VALUE);
    }

    @Test public void
    should_reject_a_quorum_size_which_is_less_than_a_majority() {
        exception.expect(IllegalStateException.class);

        int quorumSize = 1;
        proposer = PaxosProposerImpl.newProposer(learner, ImmutableList.of(acceptingAcceptor, rejectingAcceptor, rejectingAcceptor), NO_LEARNERS, quorumSize, executor);
    }

    @Test public void
    should_teach_its_learner_the_accepted_value() throws PaxosRoundFailureException {
        proposer = PaxosProposerImpl.newProposer(learner, ImmutableList.of(acceptingAcceptor), NO_LEARNERS, 1, executor);

        proposer.propose(KEY, VALUE);

        verify(learner, atLeastOnce()).learn(KEY, paxosValue());
    }

    @Test public void
    should_teach_other_learners_the_accepted_value() throws PaxosRoundFailureException {
        proposer = PaxosProposerImpl.newProposer(learner, ImmutableList.of(acceptingAcceptor), ImmutableList.of(otherLearner), 1, executor);

        proposer.propose(KEY, VALUE);

        verify(otherLearner, atLeastOnce()).learn(KEY, paxosValue());
    }

    private PaxosValue paxosValue() {
        return new PaxosValue(proposer.getUUID(), KEY, VALUE);
    }

    private PaxosPromise failedPromise() {
        return PaxosPromise.create(false, PROPOSAL_ID, null, null);
    }

    private PaxosPromise successfulPromise() {
        PaxosProposalId lastAcceptedId = null;
        PaxosValue value = null;
        return PaxosPromise.create(true, PROPOSAL_ID, lastAcceptedId, value);
    }

}
