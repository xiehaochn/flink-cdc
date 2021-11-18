package com.hawx.task;

import com.hawx.entity.event.LogEvent;
import com.hawx.source.MysqlBinlogSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MainLauncher extends BaseStreamTask {

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment streamExecutionEnvironment = initStreamEnv(parameterTool);
    DataStream<LogEvent> dataStream = streamExecutionEnvironment.addSource(new MysqlBinlogSource());
    dataStream.print();
    streamExecutionEnvironment.execute("mysqlBinlogCdc");
  }
}
