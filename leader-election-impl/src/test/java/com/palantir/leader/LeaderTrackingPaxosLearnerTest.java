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

package com.palantir.leader;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.paxos.PaxosInstanceId;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

public class LeaderTrackingPaxosLearnerTest {
    public static final int QUORUM_SIZE = 1;
    private final PaxosLearner otherLearner = mock(PaxosLearner.class);
    private final PaxosLearner knowledge = mock(PaxosLearner.class);
    public static final PaxosValue NEWER_VALUE = new PaxosValue(PaxosInstanceId.fromSeq(2), null);
    private final ImmutableList<PaxosLearner> learners = ImmutableList.of(otherLearner);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private LeaderTrackingPaxosLearner leaderTrackingPaxosLearner = new LeaderTrackingPaxosLearner(knowledge, executor);;

    @Test
    public void shouldReturnLatestValueFromUnderlyingLearner() {
        PaxosValue greatestValue = mock(PaxosValue.class);
        when(knowledge.getGreatestLearnedValue()).thenReturn(greatestValue);

        PaxosValue actual = leaderTrackingPaxosLearner.latestLeaderValue();

        assertThat(actual, equalTo(greatestValue));
    }

    @Test
    public void should_recognize_if_there_are_no_new_values_to_learn() {
        when(otherLearner.getAllLearnedValues()).thenReturn(ImmutableSet.of());

        boolean updated = leaderTrackingPaxosLearner.updateLearnedStateFromPeers(learners, QUORUM_SIZE);

        assertThat(updated, is(false));
    }

    @Test
    public void should_recognize_if_there_are_new_values_to_learn() {
        when(otherLearner.getAllLearnedValues()).thenReturn(ImmutableSet.of(NEWER_VALUE));

        boolean updated = leaderTrackingPaxosLearner.updateLearnedStateFromPeers(learners, QUORUM_SIZE);

        assertThat(updated, is(true));
    }

    @Test
    public void should_update_local_knowledge_if_there_are_new_values_to_learn() {
        when(otherLearner.getAllLearnedValues()).thenReturn(ImmutableSet.of(NEWER_VALUE));

        leaderTrackingPaxosLearner.updateLearnedStateFromPeers(learners, QUORUM_SIZE);

        verify(knowledge).learn(NEWER_VALUE);
    }

}