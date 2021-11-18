package com.hawx.task;

import com.hawx.entity.event.LogEvent;
import com.hawx.source.MysqlBinlogSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainLauncher extends BaseStreamTask {
  private static Logger logger = LoggerFactory.getLogger(MainLauncher.class);

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    logger.info("start init stream env");
    StreamExecutionEnvironment streamExecutionEnvironment = initStreamEnv(parameterTool);
    logger.info("end init stream env");
    DataStream<LogEvent> dataStream = streamExecutionEnvironment.addSource(new MysqlBinlogSource());
    dataStream.print();
    streamExecutionEnvironment.execute("mysqlBinlogCdc");
  }
}
