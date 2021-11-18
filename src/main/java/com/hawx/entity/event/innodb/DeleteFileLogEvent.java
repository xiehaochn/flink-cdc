package com.hawx.entity.event.innodb;

import com.hawx.entity.event.LogBuffer;
import com.hawx.entity.event.LogEvent;

/**
 * Delete_file_log_event.
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class DeleteFileLogEvent extends LogEvent {

  private final long fileId;

  /* DF = "Delete File" */
  public static final int DF_FILE_ID_OFFSET = 0;

  public DeleteFileLogEvent(
      LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
    super(header);

    final int commonHeaderLen = descriptionEvent.commonHeaderLen;
    buffer.position(commonHeaderLen + DF_FILE_ID_OFFSET);
    fileId = buffer.getUint32(); // DF_FILE_ID_OFFSET
  }

  public final long getFileId() {
    return fileId;
  }
}
