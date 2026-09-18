package org.mitre.synthea.simulator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ApiTest {

  @Test
  public void parseModulesReturnsNullWhenParameterIsAbsent() {
    assertNull(Api.CasualtyHandler.parseModules(Collections.emptyMap()));
  }

  @Test
  public void parseModulesAcceptsCommaSeparatedAndRepeatedValues() {
    Map<String, List<String>> params = new LinkedHashMap<>();
    params.put("modules", Arrays.asList("hypertension, asthma", "allerg*"));

    assertEquals(Arrays.asList("hypertension", "asthma", "allerg*"),
        Api.CasualtyHandler.parseModules(params));
  }

  @Test
  public void parseModulesIgnoresBlankItems() {
    Map<String, List<String>> params = new LinkedHashMap<>();
    params.put("modules", Arrays.asList("hypertension, ,", ""));

    assertEquals(Collections.singletonList("hypertension"),
        Api.CasualtyHandler.parseModules(params));
  }
}
