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

package io.hekate.spring.bean.codec;

import io.hekate.codec.CodecService;
import io.hekate.spring.bean.HekateBaseBean;

/**
 * Imports {@link CodecService} into a Spring context.
 */
public class CodecServiceBean extends HekateBaseBean<CodecService> {
    @Override
    public CodecService getObject() throws Exception {
        return getSource().codec();
    }

    @Override
    public Class<CodecService> getObjectType() {
        return CodecService.class;
    }
}
