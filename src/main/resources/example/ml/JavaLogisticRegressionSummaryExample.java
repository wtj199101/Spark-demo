/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
// $cn.spark.example on$
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
// $cn.spark.example off$

public class JavaLogisticRegressionSummaryExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaLogisticRegressionSummaryExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // Load training data
    DataFrame training = sqlContext.read().format("libsvm")
      .load("data/mllib/sample_libsvm_data.txt");

    LogisticRegression lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8);

    // Fit the model
    LogisticRegressionModel lrModel = lr.fit(training);

    // $cn.spark.example on$
    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // cn.spark.example
    LogisticRegressionTrainingSummary trainingSummary = lrModel.summary();

    // Obtain the loss per iteration.
    double[] objectiveHistory = trainingSummary.objectiveHistory();
    for (double lossPerIteration : objectiveHistory) {
      System.out.println(lossPerIteration);
    }

    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
    // classification problem.
    BinaryLogisticRegressionSummary binarySummary =
      (BinaryLogisticRegressionSummary) trainingSummary;

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    DataFrame roc = binarySummary.roc();
    roc.show();
    roc.select("FPR").show();
    System.out.println(binarySummary.areaUnderROC());

    // Get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
    // this selected threshold.
    DataFrame fMeasure = binarySummary.fMeasureByThreshold();
    double maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0);
    double bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure))
      .select("threshold").head().getDouble(0);
    lrModel.setThreshold(bestThreshold);
    // $cn.spark.example off$

    jsc.stop();
  }
}