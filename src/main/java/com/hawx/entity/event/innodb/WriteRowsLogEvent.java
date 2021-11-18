package com.hawx.entity.event.innodb;

import com.hawx.entity.event.LogBuffer;

/**
 * Log row insertions and updates. The event contain several insert/update rows for a table. Note
 * that each event contains only rows for one table.
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class WriteRowsLogEvent extends RowsLogEvent {

  public WriteRowsLogEvent(
      LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
    super(header, buffer, descriptionEvent);
  }
}
