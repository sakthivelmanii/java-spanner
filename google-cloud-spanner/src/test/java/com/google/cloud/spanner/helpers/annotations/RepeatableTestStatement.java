/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.helpers.annotations;

import org.junit.runners.model.Statement;

public class RepeatableTestStatement extends Statement {

  private final int times;
  private final Statement statement;
  public RepeatableTestStatement(int times, Statement statement) {
    this.statement = statement;
    this.times = times;
  }

  @Override
  public void evaluate() throws Throwable {
    for(int i = 0; i < times; i++) {
      statement.evaluate();
    }
  }
}