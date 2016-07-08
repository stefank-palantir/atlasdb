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

import java.io.Serializable;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonDeserialize(as = ImmutablePaxosInstanceId.class)
@JsonSerialize(as = ImmutablePaxosInstanceId.class)
public abstract class PaxosInstanceId implements Comparable<PaxosInstanceId>, Serializable {
    @Override
    public int compareTo(PaxosInstanceId o) {
        return ((Long) seq()).compareTo(o.seq());
    }

    @JsonProperty("seq")
    public abstract long seq();

    public static PaxosInstanceId fromSeq(long seq) {
        return ImmutablePaxosInstanceId.builder().seq(seq).build();
    }

    public static PaxosInstanceId fromString(String seq) {
        return fromSeq(Long.parseLong(seq));
    }

    @Override
    public String toString() {
        return String.valueOf(seq());
    }
}
