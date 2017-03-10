/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.spring.boot.task;

import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import io.hekate.task.CallableTask;
import io.hekate.task.TaskService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class HekateTaskServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    static class TaskConfig extends HekateTestConfigBase {
        @Autowired(required = false)
        private TaskService taskService;
    }

    @Test
    public void testEnabled() throws Exception {
        registerAndRefresh(new String[]{"hekate.task.enable:true"}, TaskConfig.class);

        assertNotNull(get("taskService", TaskService.class));
        assertNotNull(get(TaskConfig.class).taskService);

        assertEquals(100500, getNode().get(TaskService.class).call((CallableTask<Integer>)() -> 100500).get().intValue());
    }

    @Test
    public void testDisabledByDefault() throws Exception {
        registerAndRefresh(TaskConfig.class);

        assertFalse(getNode().has(TaskService.class));
    }
}
