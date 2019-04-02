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

package io.hekate.election;

/**
 * Listener of leader change events.
 *
 * <p>
 * Implementations of this interface can be registered by a {@link Candidate} to its {@link FollowerContext} in order to
 * get leader change notifications while candidate stays in the {@link Candidate#becomeFollower(FollowerContext) follower}
 * state.
 * </p>
 *
 * @see FollowerContext#addListener(LeaderChangeListener)
 */
public interface LeaderChangeListener {
    /**
     * Called every time when some other node wins elections and takes a group leadership.
     *
     * @param ctx Follower context.
     */
    void onLeaderChange(FollowerContext ctx);
}
