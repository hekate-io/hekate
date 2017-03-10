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

package io.hekate.codec.internal;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.util.format.ToString;

public class DefaultCodecService implements CodecService {
    private final CodecFactory<Object> codecFactory;

    public DefaultCodecService(CodecFactory<Object> codecFactory) {
        assert codecFactory != null : "Codec factory is null.";

        this.codecFactory = codecFactory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CodecFactory<T> getCodecFactory() {
        return (CodecFactory<T>)codecFactory;
    }

    @Override
    public String toString() {
        return ToString.format(CodecService.class, this);
    }
}
