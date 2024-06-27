/*
 * Copyright 2023 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.google.deploy.ops;

import com.google.api.services.compute.Compute;

public class AutoscalersImpl /*implements Compute.Autoscalers*/ {
  private final Compute compute;

  public AutoscalersImpl(Compute compute) {
    this.compute = compute;
  }

  // @Override
  public Compute.Autoscalers.Delete delete(String project, String zone, String autoscaler) {
    // Implement the actual logic to delete the autoscaler using the Compute API
    // For demonstration purposes, we'll return null here.
    return null;
  }
}
