/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.dataflow.spark;

import java.util.HashMap;
import java.util.Map;

import com.google.cloud.dataflow.sdk.transforms.PTransform;

public class TransformEvaluatorRegistry {

  /**
   * A map from PTransform class to the corresponding TransformEvaluator to use to evaluate that
   * transform.
   *
   * <p>An instance map that contains bindings for a PipelineRunner. Bindings in this map override
   * those in the default map.
   */
  private Map<Class<? extends PTransform<?, ?>>, TransformEvaluator<?>> //
  localTransformEvaluators = new HashMap<>();

  public <PT extends PTransform<?, ?>> boolean hasTransformEvaluator(Class<PT> clazz) {
    return localTransformEvaluators.containsKey(clazz);
  }

  /**
   * Returns the TransformEvaluator to use for instances of the specified PTransform class, or null
   * if none registered.
   *
   * @param <PT> TODO
   * @param transformClass TODO
   * @return TODO
   */
  @SuppressWarnings("unchecked")
  public <PT extends PTransform<?, ?>> TransformEvaluator<PT> getTransformEvaluator(
      Class<PT> transformClass) {
    return (TransformEvaluator<PT>) localTransformEvaluators.get(transformClass);
  }

  /**
   * Records that instances of the specified PTransform class should be evaluated by the
   * corresponding TransformEvaluator. Overrides any bindings specified by
   * {@link #registerDefaultTransformEvaluator}.
   *
   * @param <PT> TODO
   * @param transformClass TODO
   * @param transformEvaluator TODO
   */
  public <PT extends PTransform<?, ?>> void registerTransformEvaluator(Class<PT> transformClass,
      TransformEvaluator<PT> transformEvaluator) {
    if (localTransformEvaluators.put(transformClass, transformEvaluator) != null) {
      throw new IllegalArgumentException("defining multiple evaluators for " + transformClass);
    }
  }

  /**
   * Temporary utility method to avoid rewriting all of the translator evaluators to take a generic
   * PTransform to pass to the registry.
   * TODO: delete this and rewrite the methods.
   *
   * @param <PT> TODO
   * @param input TODO
   * @return TODO
   */
  @SuppressWarnings("unchecked")
  public static <PT extends TransformEvaluator<?>> PT convert(
      @SuppressWarnings("rawtypes") TransformEvaluator input) {
    return (PT) input;
  }
}
