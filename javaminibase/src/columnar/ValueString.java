package columnar;

public class ValueString extends ValueClass {
    private String value;

    public ValueString(String val) {
        value = val;
    }

    public String getValue() {
        return value;
    }

    public String toString() {
        return value;
    }
}
