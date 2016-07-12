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

import java.util.Collection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.PathParam;

import com.palantir.common.annotation.Inclusive;

public class OrderedPaxosLearner implements PaxosLearner {
    private final PaxosLearner delegate;

    public static OrderedPaxosLearner newLearner(PaxosLearner delegate) {
        return new OrderedPaxosLearner(delegate);
    }

    private OrderedPaxosLearner(PaxosLearner delegate) {
        this.delegate = delegate;
    }

    @Override
    public void learn(PaxosValue val) {
        delegate.learn(val);
    }

    @Nullable
    @Override
    public PaxosValue getLearnedValue(@PathParam("instance") PaxosInstanceId instanceId) {
        return delegate.getLearnedValue(instanceId);
    }

    @Nullable
    @Override
    public PaxosValue getGreatestLearnedValue() {
        return delegate.getGreatestLearnedValue();
    }

    @Nonnull
    @Override
    public Collection<PaxosValue> getLearnedValuesSince(@PathParam("instance") @Inclusive PaxosInstanceId instance) {
        return delegate.getLearnedValuesSince(instance);
    }

    @Nonnull
    @Override
    public Collection<PaxosValue> getAllLearnedValues() {
        return delegate.getAllLearnedValues();
    }
}
