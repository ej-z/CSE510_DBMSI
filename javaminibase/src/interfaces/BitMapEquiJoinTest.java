package interfaces;

/**
 * Created by dixith on 4/5/18.
 */
public class BitMapEquiJoinTest {

    public static String datafileName;
    public static String columnDBName;
    public static String columnarFileName;
    public static int numColumns;
    private static String datafileName2;
    private static String columnDBName2;
    private static String columnarFileName2;

    public static void main(String[] argvs) {

        try {
            BatchInsert insertTest = new BatchInsert();

            datafileName = "leftSampleData.txt";
            columnDBName = "R1";
            columnarFileName = "Sailors";
            numColumns = 3;

            datafileName2 = "rightSampleData.txt";
            columnDBName2 = "R2";
            columnarFileName2 = "Boats";
            numColumns = 3;
            insertTest.runTests();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}
