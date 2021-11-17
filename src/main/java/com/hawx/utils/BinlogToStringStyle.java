package com.hawx.utils;

import org.apache.commons.lang3.builder.ToStringStyle;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BinlogToStringStyle extends ToStringStyle {
  private static final long serialVersionUID = -6568177374288222145L;

  protected void appendDetail(StringBuffer buffer, String fieldName, Object value) {
    // 增加自定义的date对象处理
    if (value instanceof Date) {
      value = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value);
    }
    // 后续可以增加其他自定义对象处理
    buffer.append(value);
  }
}
