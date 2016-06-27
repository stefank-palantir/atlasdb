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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
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

    private static final long KEY = 1;
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

    @Before
    public void setup() {
        when(acceptingAcceptor.prepare(eq(KEY), any(PaxosProposalId.class))).thenReturn(successfulPromise());
        when(acceptingAcceptor.accept(eq(KEY), any(PaxosProposal.class))).thenReturn(SUCCESSFUL_ACCEPTANCE);

        when(rejectingAcceptor.prepare(eq(KEY), any(PaxosProposalId.class))).thenReturn(failedPromise());
        when(rejectingAcceptor.accept(eq(KEY), any(PaxosProposal.class))).thenReturn(FAILED_ACCEPTANCE);

        when(promiseThenRejectAcceptor.prepare(eq(KEY), any(PaxosProposalId.class))).thenReturn(successfulPromise());
        when(promiseThenRejectAcceptor.accept(eq(KEY), any(PaxosProposal.class))).thenReturn(FAILED_ACCEPTANCE);
    }

    @Test public void
    should_accept_a_proposal_if_there_is_only_one_acceptor_and_the_acceptor_accepts_the_proposal() throws PaxosRoundFailureException {
        PaxosProposer proposer = createProposerWithAcceptors(ImmutableList.of(acceptingAcceptor));

        assertThat(proposer.propose(KEY, VALUE), is(VALUE));
    }

    @Test public void
    should_accept_a_proposal_if_there_are_3_acceptors_2_accept_and_1_rejects() throws PaxosRoundFailureException {
        PaxosProposer proposer = createProposerWithAcceptors(ImmutableList.of(acceptingAcceptor, acceptingAcceptor, rejectingAcceptor));

        assertThat(proposer.propose(KEY, VALUE), is(VALUE));
    }

    @Test public void
    should_reject_a_proposal_if_there_are_3_acceptors_1_accept_and_2_rejects() throws PaxosRoundFailureException {
        exception.expect(PaxosRoundFailureException.class);

        PaxosProposer proposer = createProposerWithAcceptors(ImmutableList.of(acceptingAcceptor, rejectingAcceptor, rejectingAcceptor));

        proposer.propose(KEY, VALUE);
    }

    @Test public void
    should_reject_a_proposal_if_there_are_3_acceptors_1_accepts_and_2_promises_then_rejects() throws PaxosRoundFailureException {
        exception.expect(PaxosRoundFailureException.class);

        PaxosProposer proposer = createProposerWithAcceptors(ImmutableList.of(acceptingAcceptor, promiseThenRejectAcceptor, promiseThenRejectAcceptor));

        proposer.propose(KEY, VALUE);
    }

    @Test public void
    should_reject_a_quorum_size_which_is_less_than_a_majority() {
        exception.expect(IllegalStateException.class);

        int quorumSize = 1;
        PaxosProposerImpl.newProposer(learner, ImmutableList.of(acceptingAcceptor, rejectingAcceptor, rejectingAcceptor), NO_LEARNERS, quorumSize, executor);
    }

    @Test public void
    should_teach_its_learner_the_accepted_value() throws PaxosRoundFailureException {
        PaxosProposer proposer = createProposerWithAcceptors(ImmutableList.of(acceptingAcceptor));

        proposer.propose(KEY, VALUE);

        verify(learner, atLeastOnce()).learn(KEY, paxosValueFor(proposer));
    }

    @Test public void
    should_teach_other_learners_the_accepted_value() throws PaxosRoundFailureException {
        PaxosProposer proposer = PaxosProposerImpl.newProposer(learner, ImmutableList.of(acceptingAcceptor), ImmutableList.of(otherLearner), 1, executor);

        proposer.propose(KEY, VALUE);

        verify(otherLearner, atLeastOnce()).learn(KEY, paxosValueFor(proposer));
    }

    @Test public void
    should_increase_proposal_id_on_failure() {
        PaxosProposer proposer = createProposerWithAcceptors(ImmutableList.of(acceptingAcceptor, rejectSmallProposalIdsAcceptor(10), rejectSmallProposalIdsAcceptor(10)));

        boolean success = false;
        for (int attempts = 0; attempts < 11; attempts++) {
            try {
                proposer.propose(KEY, VALUE);
                success = true;
            } catch (PaxosRoundFailureException e) {
                // try again
            }
        }
        assertThat(success, is(true));
    }

    private PaxosProposer createProposerWithAcceptors(ImmutableList<PaxosAcceptor> acceptors) {
        int quorumSize = acceptors.size() / 2 + 1;
        return PaxosProposerImpl.newProposer(learner, acceptors, NO_LEARNERS, quorumSize, executor);
    }

    private PaxosAcceptor rejectSmallProposalIdsAcceptor(long lastPromisedNumber) {
        PaxosAcceptor acceptor = mock(PaxosAcceptor.class);

        when(acceptor.prepare(Matchers.anyLong(), argThat(hasProposalNumber(lessThan(lastPromisedNumber))))).thenReturn(failedPromise());
        when(acceptor.prepare(Matchers.anyLong(), argThat(hasProposalNumber(greaterThanOrEqualTo(lastPromisedNumber))))).thenReturn(successfulPromise());

        when(acceptor.accept(Matchers.anyLong(), any(PaxosProposal.class))).thenReturn(SUCCESSFUL_ACCEPTANCE);

        return acceptor;
    }

    private Matcher<PaxosProposalId> hasProposalNumber(Matcher<Long> subMatcher) {
        return new FeatureMatcher<PaxosProposalId, Long>(subMatcher, "proposal number", "proposal number") {

            @Override
            protected Long featureValueOf(PaxosProposalId actual) {
                return actual.getNumber();
            }
        };
    }

    private PaxosValue paxosValueFor(PaxosProposer proposer) {
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
