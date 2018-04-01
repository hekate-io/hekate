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

package io.hekate.metrics.cloudwatch;

import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class CloudWatchClientRequestCaptor implements CloudWatchMetricsPublisher.CloudWatchClient {
    private List<PutMetricDataRequest> requests = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void putMetrics(PutMetricDataRequest request) {
        requests.add(request);
    }

    public List<PutMetricDataRequest> requests() {
        return requests;
    }
}
