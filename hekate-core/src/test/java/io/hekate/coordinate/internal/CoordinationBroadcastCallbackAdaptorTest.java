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

package io.hekate.coordinate.internal;

import io.hekate.HekateTestBase;
import io.hekate.coordinate.CoordinationBroadcastCallback;
import io.hekate.coordinate.CoordinationMember;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CoordinationBroadcastCallbackAdaptorTest extends HekateTestBase {
    @Test
    public void testOnResponses() throws Exception {
        repeat(3, i -> {
            int membersSize = i + 1;

            CoordinationBroadcastCallback callback = mock(CoordinationBroadcastCallback.class);

            BroadcastCallbackAdaptor adaptor = new BroadcastCallbackAdaptor(membersSize, callback);

            ArgumentCaptor<Map<CoordinationMember, Object>> captor = newResultCaptor();

            List<CoordinationMember> members = new ArrayList<>();

            for (int j = 0; j < membersSize; j++) {
                CoordinationMember member = mock(CoordinationMember.class);

                members.add(member);

                adaptor.onResponse("test" + j, member);
            }

            verify(callback).onResponses(captor.capture());

            assertEquals(membersSize, captor.getValue().size());

            for (int j = 0; j < membersSize; j++) {
                assertEquals("test" + j, captor.getValue().get(members.get(j)));
            }

            verifyNoMoreInteractions(callback);
        });
    }

    @Test
    public void testOnCancel() throws Exception {
        CoordinationBroadcastCallback callback = mock(CoordinationBroadcastCallback.class);

        BroadcastCallbackAdaptor adaptor = new BroadcastCallbackAdaptor(3, callback);

        ArgumentCaptor<Map<CoordinationMember, Object>> captor = newResultCaptor();

        adaptor.onCancel();

        verify(callback).onCancel(captor.capture());

        assertEquals(0, captor.getValue().size());

        adaptor.onCancel();
        adaptor.onCancel();

        adaptor.onResponse("test", mock(CoordinationMember.class));
        adaptor.onResponse("test", mock(CoordinationMember.class));
        adaptor.onResponse("test", mock(CoordinationMember.class));

        verifyNoMoreInteractions(callback);
    }

    @Test
    public void testOnCancelPartial() throws Exception {
        CoordinationBroadcastCallback callback = mock(CoordinationBroadcastCallback.class);

        BroadcastCallbackAdaptor adaptor = new BroadcastCallbackAdaptor(3, callback);

        ArgumentCaptor<Map<CoordinationMember, Object>> captor = newResultCaptor();

        CoordinationMember m1 = mock(CoordinationMember.class);
        CoordinationMember m2 = mock(CoordinationMember.class);

        adaptor.onResponse("test1", m1);
        adaptor.onResponse("test2", m2);

        adaptor.onCancel();

        verify(callback).onCancel(captor.capture());

        assertEquals(2, captor.getValue().size());
        assertEquals("test1", captor.getValue().get(m1));
        assertEquals("test2", captor.getValue().get(m2));

        adaptor.onCancel();

        adaptor.onResponse("test3", mock(CoordinationMember.class));

        verifyNoMoreInteractions(callback);
    }

    @SuppressWarnings("unchecked")
    private ArgumentCaptor<Map<CoordinationMember, Object>> newResultCaptor() {
        return (ArgumentCaptor)ArgumentCaptor.forClass(Map.class);
    }
}
