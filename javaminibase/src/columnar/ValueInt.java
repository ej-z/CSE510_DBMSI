package columnar;

public class ValueInt extends ValueClass {
    private Integer value;

    public ValueInt(Integer val) {
        value = val;
    }

    public Integer getValue() {
        return value;
    }

    public String toString() {
        return String.valueOf(value);
    }
}