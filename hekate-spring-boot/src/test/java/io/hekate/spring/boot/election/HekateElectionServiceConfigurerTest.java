/*
 * Copyright 2019 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.spring.boot.election;

import io.hekate.election.Candidate;
import io.hekate.election.CandidateConfig;
import io.hekate.election.ElectionService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class HekateElectionServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class LeaderTestConfig extends HekateTestConfigBase {
        @Autowired
        private ElectionService electionService;

        @Bean
        public CandidateConfig candidate() {
            return new CandidateConfig().withGroup("test").withCandidate(mock(Candidate.class));
        }
    }

    @Test
    public void testLeader() throws Exception {
        registerAndRefresh(LeaderTestConfig.class);

        assertNotNull(get(LeaderTestConfig.class).electionService);
        assertNotNull(get("electionService", ElectionService.class));

        assertEquals(getNode().localNode(), getNode().election().leader("test").get());
    }
}
