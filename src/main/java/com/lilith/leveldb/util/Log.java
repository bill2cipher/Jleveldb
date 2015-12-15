package com.lilith.leveldb.util;

import org.apache.log4j.Logger;


public class Log {
  public static Logger log = Logger.getLogger("leveldb");
  
  public static void debug(String message) {
    log.debug(message);
  }
  
  public static void info(String message) {
    log.info(message);
  }
  
  public static void error(String message) {
    log.error(message);
  }
  
  public static void fatal(String message) {
    log.fatal(message);
  }
}
