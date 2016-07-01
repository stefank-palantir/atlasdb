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

public class PaxosKeyValueStore {
    private final PaxosProposer proposer;
    final PaxosLearner knowledge;

    public PaxosKeyValueStore(PaxosProposer proposer, PaxosLearner knowledge) {
        this.proposer = proposer;
        this.knowledge = knowledge;
    }

    public byte[] propose(PaxosKey key, byte[] value) throws PaxosRoundFailureException {
        return proposer.propose(key, value);
    }

    public PaxosValue getGreatestLearnedValue() {
        return knowledge.getGreatestLearnedValue();
    }
}
