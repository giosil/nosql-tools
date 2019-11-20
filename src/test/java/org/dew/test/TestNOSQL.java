package org.dew.test;

import org.dew.nosql.CommandNoSQL;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestNOSQL extends TestCase {
  
  public TestNOSQL(String testName) {
    super(testName);
  }
  
  public static Test suite() {
    return new TestSuite(TestNOSQL.class);
  }
  
  public void testApp() {
    CommandNoSQL.printHelp();
  }
  
}
