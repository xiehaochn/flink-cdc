package com.hawx.entity;

import java.io.Serializable;

public class BinlogPosition implements Serializable {

  private static final long serialVersionUID = -250506968622509083L;
  private String fileName;
  private Long position;

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public Long getPosition() {
    return position;
  }

  public void setPosition(Long position) {
    this.position = position;
  }

  public BinlogPosition(String fileName, Long position) {
    this.fileName = fileName;
    this.position = position;
  }
}
