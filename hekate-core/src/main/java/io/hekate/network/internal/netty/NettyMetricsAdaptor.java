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

package io.hekate.network.internal.netty;

interface NettyMetricsAdaptor {
    String BYTES_OUT = "bytes.out";

    String BYTES_IN = "bytes.in";

    String MSG_IN = "msg.in";

    String MSG_OUT = "msg.out";

    String MSG_QUEUE = "msg.queue";

    String MSG_ERR = "msg.err";

    String CONN_ACTIVE = "conn.active";

    NettyMetricsCallback createCallback(boolean server, String protocol);
}
