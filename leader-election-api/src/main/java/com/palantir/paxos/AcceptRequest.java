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

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = ImmutableAcceptRequest.class)
@JsonDeserialize(as = ImmutableAcceptRequest.class)
@Value.Immutable
public abstract class AcceptRequest {
    public abstract PaxosKey getKey();

    public abstract PaxosProposal getProposal();

    public static AcceptRequest from(PaxosKey key, PaxosProposal proposal) {
        return ImmutableAcceptRequest.builder().key(key).proposal(proposal).build();
    }
}
