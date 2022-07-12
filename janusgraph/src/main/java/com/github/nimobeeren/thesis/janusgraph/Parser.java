package com.github.nimobeeren.thesis.janusgraph;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class Parser {
  public static void main(String[] args) throws FileNotFoundException, IOException {
    Reader in = new FileReader(args[0]);
    CSVFormat parser = CSVFormat.Builder.create().setHeader().setSkipHeaderRecord(true).build();
    Iterable<CSVRecord> records = parser.parse(in);
    Iterator<CSVRecord> it = records.iterator();

    int numRead = 0;
    while (numRead < 1000 && it.hasNext()) {
      System.out.println(it.next().get("title"));
      numRead++;
    }
  }
}
