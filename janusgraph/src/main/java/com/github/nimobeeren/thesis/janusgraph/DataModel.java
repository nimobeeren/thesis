package com.github.nimobeeren.thesis.janusgraph;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.idmanagement.IDManager;

public abstract class DataModel {
  JanusGraph graph;
  Map<String, String> filePathByVertex = new HashMap<String, String>();
  Map<String, String> filePathByEdge = new HashMap<String, String>();
  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  DataModel(JanusGraph graph) {
    this.graph = graph;
  }

  Iterable<CSVRecord> parseFile(File dir, String fileName) throws IOException {
    CSVFormat parser =
        CSVFormat.Builder.create().setHeader().setSkipHeaderRecord(true).setNullString("").build();
    return parser.parse(new FileReader(new File(dir, fileName)));
  }

  List<Object> parsePropertyValues(PropertyKey propKey, String rawValue) throws ParseException {
    if (rawValue == null) {
      return new ArrayList<Object>();
    }

    // Split the value into multiple values if needed
    List<String> splitValues = new ArrayList<String>();
    if (propKey.cardinality() == Cardinality.LIST) {
      splitValues.addAll(Arrays.asList(rawValue.replaceAll("[\\[\\]\"]", "").split(",")));
    } else {
      splitValues.add(rawValue);
    }

    // Parse the string values if needed
    // For anything other than dates, the value is a string which is fine
    List<Object> values = new ArrayList<Object>(splitValues);
    if (propKey.dataType() == Date.class) {
      values = new ArrayList<Object>();
      for (String splitValue : splitValues) {
        values.add(dateFormat.parse(splitValue));
      }
    }

    return values;
  }

  Long parseId(IDManager idManager, String idString) throws ParseException {
    Long longId = Long.parseLong(idString);
    if (longId == 0) {
      // HACK: IDs must be positive, so let's try this instead and hope no vertex has that ID
      return idManager.toVertexId(999999999l);
    } else {
      return idManager.toVertexId(longId);
    }
  }

  public void load(File dataDir) throws IOException, ParseException {
    loadSchema();
    loadData(dataDir);
  }

  abstract void loadSchema();

  abstract boolean validate();

  abstract void loadData(File dataDir) throws IOException, ParseException;
}
