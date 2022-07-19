package com.github.nimobeeren.thesis.janusgraph;

public class Util {
  static String capitalize(String str) {
    if (str.length() == 0) {
      return str;
    }
    return str.substring(0, 1).toUpperCase() + str.substring(1);
  }
}
