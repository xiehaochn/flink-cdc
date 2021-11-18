package com.hawx.entity.event.innodb;

import com.hawx.entity.event.LogBuffer;
import com.hawx.entity.event.LogEvent;

/**
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6
 */
public class PreviousGtidsLogEvent extends LogEvent {

  public PreviousGtidsLogEvent(
      LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
    super(header);
    // do nothing , just for mysql gtid search function
  }
}
