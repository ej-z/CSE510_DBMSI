package columnar;

public abstract class ValueClass extends java.lang.Object {

}

class ValueInt extends ValueClass {
    int obj;

    ValueInt() {

    }

    ValueInt(int str) {
        obj = str;
    }
}

class ValueStr extends ValueClass {
    String obj;

    ValueStr() {

    }

    ValueStr(String str) {
        obj = str;
    }
}

class ValueFloat extends ValueClass {
    float obj;

    ValueFloat() {

    }

    ValueFloat(float str) {
        obj = str;
    }
}