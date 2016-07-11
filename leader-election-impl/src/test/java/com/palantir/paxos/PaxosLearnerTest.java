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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosLearnerTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String logPath;
    private PaxosLearner learner;

    @Before
    public void setUp() throws Exception {
        logPath = folder.newFolder("log").getAbsolutePath();
        learner = PaxosLearnerImpl.newLearner(logPath);
    }

    @Test
    public void should_have_no_greatest_learned_value_on_startup() throws IOException {
        assertThat(learner.getGreatestLearnedValue(), is(nullValue()));
    }

    @Test
    public void should_return_null_for_unlearned_value() {
        assertThat(learner.getLearnedValue(PaxosInstanceId.fromSeq(1L)), is(nullValue()));
    }

    @Test
    public void should_learn_single_value() {
        long seq = 1L;
        PaxosValue val = new PaxosValue(PaxosInstanceId.fromSeq(seq), "blah".getBytes());
        learner.learn(val);

        assertThat(learner.getLearnedValue(PaxosInstanceId.fromSeq(seq)), is(val));
        assertThat(learner.getGreatestLearnedValue(), is(val));
    }

    @Test
    public void should_still_return_null_after_learning_other_value() {
        long seq = 0L;
        learnBlahFor(seq);

        assertThat(learner.getLearnedValue(PaxosInstanceId.fromSeq(1L)), is(nullValue()));
    }

    @Test
    public void should_learn_out_of_order_value_and_still_return_greatest() {
        PaxosValue higherValue = learnBlahFor(1L);

        assertThat(learner.getGreatestLearnedValue(), is(higherValue));

        learnBlahFor(0L);

        assertThat(learner.getGreatestLearnedValue(), is(higherValue));
    }

    @Test
    public void should_update_greatest_learned_value() {
        PaxosValue lowerValue = learnBlahFor(0L);
        assertThat(learner.getGreatestLearnedValue(), is(lowerValue));

        PaxosValue higherValue = learnBlahFor(1L);
        assertThat(learner.getGreatestLearnedValue(), is(higherValue));
    }

    @Test
    public void should_get_no_learned_values_if_never_learned_a_value() {
        assertThat(learner.getLearnedValuesSince(PaxosInstanceId.fromSeq(1L)), empty());
    }

    @Test
    public void should_get_no_learned_values_since_greater_value() {
        learnBlahFor(0L);

        assertThat(learner.getLearnedValuesSince(PaxosInstanceId.fromSeq(1L)), empty());
    }

    @Test
    public void should_get_learned_values_inclusively() {
        PaxosValue paxosValue = learnBlahFor(1L);

        assertThat(learner.getLearnedValuesSince(PaxosInstanceId.fromSeq(1L)), contains(paxosValue));
    }

    @Test
    public void should_get_multiple_learned_values() {
        PaxosValue lowerValue = learnBlahFor(0L);
        PaxosValue higherValue = learnBlahFor(2L);

        assertThat(learner.getLearnedValuesSince(PaxosInstanceId.fromSeq(0L)), containsInAnyOrder(lowerValue, higherValue));
    }


    @Test
    public void should_ensure_sequence_persists_between_objects() {
        PaxosValue toLearnFromLog = learnBlahFor(0L);

        PaxosLearner fromLog = PaxosLearnerImpl.newLearner(logPath);
        assertThat(fromLog.getLearnedValue(PaxosInstanceId.fromSeq(0L)), is(toLearnFromLog));
    }

    @Test
    public void should_persist_greatest_learned_value_between_objects() {
        PaxosValue toLearnFromLog = learnBlahFor(0L);

        PaxosLearner fromLog = PaxosLearnerImpl.newLearner(logPath);
        assertThat(fromLog.getGreatestLearnedValue(), is(toLearnFromLog));
    }

    @Test
    public void should_persist_learned_values_between_objects() {
        PaxosValue lowerValue = learnBlahFor(0L);
        PaxosValue higherValue = learnBlahFor(1L);

        PaxosLearner fromLog = PaxosLearnerImpl.newLearner(logPath);
        long seq = 0L;
        assertThat(fromLog.getLearnedValuesSince(PaxosInstanceId.fromSeq(seq)), containsInAnyOrder(lowerValue, higherValue));
    }

    @Test
    public void should_get_all_learned_values_multiple_values() {
        PaxosValue firstValue = learnBlahFor(0L);
        PaxosValue secondValue = learnBlahFor(1L);
        assertThat(learner.getAllLearnedValues(), containsInAnyOrder(firstValue, secondValue));
    }

    private PaxosValue learnBlahFor(long seq) {
        String blah = "blah" + seq;
        PaxosValue val = new PaxosValue(PaxosInstanceId.fromSeq(seq), blah.getBytes());
        learner.learn(val);
        return val;
    }
}