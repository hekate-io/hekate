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

import io.hekate.coordinate.CoordinationMember;
import io.hekate.coordinate.CoordinationRequest;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.Message;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;

class DefaultCoordinationRequest implements CoordinationRequest {
    @ToStringIgnore
    private final String process;

    @ToStringIgnore
    private final CoordinationMember from;

    @ToStringIgnore
    private final Message<CoordinationProtocol> msg;

    private final CoordinationProtocol.Request request;

    public DefaultCoordinationRequest(String process, CoordinationMember from, Message<CoordinationProtocol> msg) {
        assert process != null : "Process name is null.";
        assert from != null : "Member is null.";
        assert msg != null : "Message is null.";

        this.process = process;
        this.from = from;
        this.msg = msg;

        this.request = msg.payload(CoordinationProtocol.Request.class);
    }

    @Override
    public CoordinationMember from() {
        return from;
    }

    @Override
    public Object get() {
        return request.request();
    }

    @Override
    public <T> T get(Class<T> type) {
        return type.cast(request.request());
    }

    @Override
    public void reply(Object response) {
        ArgAssert.notNull(response, "Response");

        msg.reply(new CoordinationProtocol.Response(process, response));
    }

    @Override
    public String toString() {
        return ToString.format(CoordinationRequest.class, this);
    }
}
