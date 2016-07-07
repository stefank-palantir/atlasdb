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

import static org.hamcrest.MatcherAssert.assertThat;
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
    public void newLearnerHasNoGreatestLearnedValue() throws IOException {
        assertThat(learner.getGreatestLearnedValue(), is(nullValue()));
    }

    @Test
    public void newLearnerReturnsNullForGivenLearnedValue() {
        assertThat(learner.getLearnedValue(1L), is(nullValue()));
    }

    @Test
    public void canLearnSingle() {
        long seq = 1L;
        PaxosValue val = new PaxosValue(PaxosKey.fromSeq(seq), "blah".getBytes());
        learner.learn(seq, val);

        assertThat(learner.getLearnedValue(seq), is(val));
        assertThat(learner.getGreatestLearnedValue(), is(val));
    }

    @Test
    public void getLearnedValueStillReturnsNullAfterLearningOthervalue() {
        long seq = 0L;
        learnBlahFor(seq);

        assertThat(learner.getLearnedValue(1L), is(nullValue()));
    }

    @Test
    public void canLearnOutOfOrderValuesAndStillReturnGreatest() {
        PaxosValue higherValue = learnBlahFor(1L);

        assertThat(learner.getGreatestLearnedValue(), is(higherValue));

        learnBlahFor(0L);

        assertThat(learner.getGreatestLearnedValue(), is(higherValue));
    }

    @Test
    public void updatesGreatestLearnedValue() {
        PaxosValue lowerValue = learnBlahFor(0L);
        assertThat(learner.getGreatestLearnedValue(), is(lowerValue));

        PaxosValue higherValue = learnBlahFor(1L);
        assertThat(learner.getGreatestLearnedValue(), is(higherValue));
    }

    private PaxosValue learnBlahFor(long seq) {
        String blah = "blah" + seq;
        PaxosValue val = new PaxosValue(PaxosKey.fromSeq(seq), blah.getBytes());
        learner.learn(seq, val);
        return val;
    }
}