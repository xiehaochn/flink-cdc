package com.hawx.entity;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class DataBase implements Serializable {

  private static final long serialVersionUID = 5072478257774007161L;
  private String type;
  private String host;
  private int port;
  private String url;
  private String user;
  private String password;
  private String defaultDb;

  public DataBase(
      String type,
      String host,
      int port,
      String url,
      String user,
      String password,
      String defaultDb) {
    this.type = type;
    this.host = host;
    this.port = port;
    this.url = url;
    this.user = user;
    this.password = password;
    this.defaultDb = defaultDb;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDefaultDb() {
    return defaultDb;
  }

  public void setDefaultDb(String defaultDb) {
    this.defaultDb = defaultDb;
  }

  public InetSocketAddress getInetSocketAddress() {
    return new InetSocketAddress(host, port);
  }
}
