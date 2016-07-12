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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

public class LeaderTrackingPaxosLearnerTest {

    @Test
    public void shouldReturnLatestValueFromUnderlyingLearner() {
        PaxosLearner knowledge = mock(PaxosLearner.class);
        PaxosValue greatestValue = mock(PaxosValue.class);
        when(knowledge.getGreatestLearnedValue()).thenReturn(greatestValue);
        LeaderTrackingPaxosLearner leaderTrackingPaxosLearner = new LeaderTrackingPaxosLearner(knowledge);

        PaxosValue actual = leaderTrackingPaxosLearner.latestLeaderValue();

        assertThat(actual, equalTo(greatestValue));
    }

}