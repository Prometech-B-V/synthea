package org.mitre.synthea.world.geography;

import com.google.gson.JsonObject;

public class PointOfInterest {
    public String name;
    public String label;
    public boolean contaminated;
    public long startTime;
    public long endTime;
    public JsonObject properties;

    public PointOfInterest(String name, String label, boolean contaminated, JsonObject properties) {
        this.name = name;
        this.label = label;
        this.contaminated = contaminated;
        this.properties = properties;
    }
}
