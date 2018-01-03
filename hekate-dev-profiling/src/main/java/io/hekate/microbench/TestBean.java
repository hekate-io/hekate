/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.microbench;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TestBean implements Serializable {
    private static final long serialVersionUID = 1;

    private long longVal1;

    private long longVal2;

    private long longVal3;

    private long longVal4;

    private long longVal5;

    private String strVal1;

    private String strVal2;

    private String strVal3;

    private String strVal4;

    private String strVal5;

    private List<TestBean> children;

    public static TestBean random() {
        return doRandom(true);
    }

    public long getLongVal1() {
        return longVal1;
    }

    public long getLongVal2() {
        return longVal2;
    }

    public long getLongVal3() {
        return longVal3;
    }

    public long getLongVal4() {
        return longVal4;
    }

    public long getLongVal5() {
        return longVal5;
    }

    public String getStrVal1() {
        return strVal1;
    }

    public String getStrVal2() {
        return strVal2;
    }

    public String getStrVal3() {
        return strVal3;
    }

    public String getStrVal4() {
        return strVal4;
    }

    public String getStrVal5() {
        return strVal5;
    }

    public List<TestBean> getChildren() {
        return children;
    }

    private static TestBean doRandom(boolean withChildren) {
        TestBean arg = new TestBean();

        arg.longVal1 = ThreadLocalRandom.current().nextLong();
        arg.longVal2 = ThreadLocalRandom.current().nextLong();
        arg.longVal3 = ThreadLocalRandom.current().nextLong();
        arg.longVal4 = ThreadLocalRandom.current().nextLong();
        arg.longVal5 = ThreadLocalRandom.current().nextLong();
        arg.strVal1 = String.valueOf(ThreadLocalRandom.current().nextLong());
        arg.strVal2 = String.valueOf(ThreadLocalRandom.current().nextLong());
        arg.strVal3 = String.valueOf(ThreadLocalRandom.current().nextLong());
        arg.strVal4 = String.valueOf(ThreadLocalRandom.current().nextLong());
        arg.strVal5 = String.valueOf(ThreadLocalRandom.current().nextLong());

        if (withChildren) {
            List<TestBean> children = new ArrayList<>(5);

            for (int i = 0; i < 5; i++) {
                children.add(doRandom(false));
            }

            arg.children = children;
        }

        return arg;
    }
}
