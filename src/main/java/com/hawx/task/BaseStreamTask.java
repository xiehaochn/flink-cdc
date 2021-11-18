package com.hawx.task;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class BaseStreamTask {
  private static final int MIN_PARALLELISM = 1;
  private static final int MAX_PARALLELISM = 20;
  private static final int DEFAULT_CHECK_POINT_INTERVAL_SECONDS = 600;

  public static StreamExecutionEnvironment initStreamEnv(ParameterTool parameterTool) {
    return StreamExecutionEnvironment.getExecutionEnvironment()
        .enableCheckpointing(
            TimeUnit.SECONDS.toMillis(DEFAULT_CHECK_POINT_INTERVAL_SECONDS),
            CheckpointingMode.EXACTLY_ONCE);
  }
}
