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

import static com.google.common.collect.ImmutableList.copyOf;

import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import com.google.common.base.Defaults;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.palantir.paxos.ImmutablePaxosInstanceId;
import com.palantir.paxos.PaxosInstanceId;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosUpdate;
import com.palantir.paxos.PaxosValue;

public class LeaderTrackingPaxosLearner {

    final PaxosLearner knowledge;
    final ExecutorService executor;

    public LeaderTrackingPaxosLearner(PaxosLearner knowledge, ExecutorService executor) {
        this.knowledge = knowledge;
        this.executor = executor;
    }

    public PaxosValue latestLeaderValue() {
        return knowledge.getGreatestLearnedValue();
    }

    public PaxosInstanceId generateNextPaxosInstanceId(PaxosValue lastValue) {
        // TODO might be able to get lastValue from latestLeaderValue(), but need to first check for concurrency implications where this is called
        long seq;
        if (lastValue != null) {
            seq = lastValue.getRound().seq() + 1;
        } else {
            seq = Defaults.defaultValue(long.class);
        }

        return ImmutablePaxosInstanceId.builder().seq(seq).build();
    }


    /**
     * Queries all other learners for unknown learned values
     *
     * @param numPeersToQuery number of peer learners to query for updates
     * @returns true if new state was learned, otherwise false
     */
    public boolean updateLearnedStateFromPeers(ImmutableList<PaxosLearner> learners, int quorumSize) {
        List<PaxosUpdate> updates = PaxosQuorumChecker.collectQuorumResponses(
                learners,
                new Function<PaxosLearner, PaxosUpdate>() {
                    @Override
                    @Nullable
                    public PaxosUpdate apply(@Nullable PaxosLearner learner) {
                        return new PaxosUpdate(
                                copyOf(learner.getAllLearnedValues()));
                    }
                },
                quorumSize,
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS);

        // learn the state accumulated from peers
        boolean learned = false;
        for (PaxosUpdate update : updates) {
            ImmutableCollection<PaxosValue> values = update.getValues();
            for (PaxosValue value : values) {
                PaxosValue currentLearnedValue = knowledge.getLearnedValue(value.getRound());
                if (currentLearnedValue == null) {
                    knowledge.learn(value);
                    learned = true;
                }
            }
        }

        return learned;
    }

}
