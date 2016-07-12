package com.palantir.leader;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosInstanceId;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosValue;

public class PaxosLeaderElectionServiceTest {

    private PaxosLeaderElectionService electionService;
    private final PaxosLearner otherLearner = mock(PaxosLearner.class);
    private final PaxosLearner knowledge = mock(PaxosLearner.class);
    public static final PaxosValue FIRST_VALUE = new PaxosValue(PaxosInstanceId.fromSeq(0), null);
    public static final PaxosValue OTHER_VALUE = new PaxosValue(PaxosInstanceId.fromSeq(2), null);

    @Before
    public void setup() {
        PaxosProposer proposer = mock(PaxosProposer.class);
        Map<PingableLeader, HostAndPort> potentialLeadersToHosts = ImmutableMap.of();
        List<PaxosAcceptor> acceptors = ImmutableList.of();
        List<PaxosLearner> learners = ImmutableList.of(otherLearner);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        long updatePollingWainInMs = 0;
        long randomWaitBeforeProposingLeadership = 0;
        long leaderPingResponseWaitMs = 0;
        electionService = new PaxosLeaderElectionService(proposer, knowledge, potentialLeadersToHosts, acceptors, learners, executor, updatePollingWainInMs, randomWaitBeforeProposingLeadership, leaderPingResponseWaitMs);

        when(proposer.getQuorumSize()).thenReturn(1);
    }

    @Test
    public void should_recognize_if_there_are_no_new_values_to_learn() {
        when(otherLearner.getLearnedValuesSince(any(PaxosInstanceId.class))).thenReturn(ImmutableSet.of());

        boolean updated = electionService.updateLearnedStateFromPeers(FIRST_VALUE);

        assertThat(updated, is(false));
    }

    @Test
    public void should_recognize_if_there_are_new_values_to_learn() {
        when(otherLearner.getLearnedValuesSince(any(PaxosInstanceId.class))).thenReturn(ImmutableSet.of(OTHER_VALUE));

        boolean updated = electionService.updateLearnedStateFromPeers(FIRST_VALUE);

        assertThat(updated, is(true));
    }

    @Test
    public void should_update_local_knowledge_if_there_are_new_values_to_learn() {
        when(otherLearner.getLearnedValuesSince(any(PaxosInstanceId.class))).thenReturn(ImmutableSet.of(OTHER_VALUE));

        electionService.updateLearnedStateFromPeers(FIRST_VALUE);

        verify(knowledge).learn(OTHER_VALUE);
    }
}
