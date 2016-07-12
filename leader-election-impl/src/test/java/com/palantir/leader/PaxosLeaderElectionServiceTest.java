package com.palantir.leader;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    @Test
    public void should_update_if_there_are_new_values_to_learn() {
        PaxosLearner otherLearner = mock(PaxosLearner.class);
        when(otherLearner.getLearnedValuesSince(any(PaxosInstanceId.class))).thenReturn(ImmutableSet.of(new PaxosValue(PaxosInstanceId.fromSeq(2), null)));

        PaxosProposer proposer = mock(PaxosProposer.class);
        when(proposer.getQuorumSize()).thenReturn(1);
        PaxosLearner knowledge = mock(PaxosLearner.class);
        Map<PingableLeader, HostAndPort> potentialLeadersToHosts = ImmutableMap.of();
        List<PaxosAcceptor> acceptors = ImmutableList.of();
        List<PaxosLearner> learners = ImmutableList.of(otherLearner);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        long updatePollingWainInMs = 0;
        long randomWaitBeforeProposingLeadership = 0;
        long leaderPingResponseWaitMs = 0;
        PaxosLeaderElectionService electionService = new PaxosLeaderElectionService(proposer, knowledge, potentialLeadersToHosts, acceptors, learners, executor, updatePollingWainInMs, randomWaitBeforeProposingLeadership, leaderPingResponseWaitMs);

        PaxosValue greatestLearned = new PaxosValue(PaxosInstanceId.fromSeq(0), null);
        assertThat(electionService.updateLearnedStateFromPeers(greatestLearned), is(true));
    }
}
