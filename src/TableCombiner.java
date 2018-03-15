import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.apache.commons.io.FilenameUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class TableCombiner {
    private final String ERROR_TABLE_INVALID = " Table Invalid.";
    private final String ERROR_TABLE_CONFLICT = " Table conflict.";
    private final String ERROR_TABLE_WITHOUT_ID = " Table without ID.";
    private final String ERROR_TABLE_INVALID_ID = " Table with invalid ID.";

    private Table<String, String, String> table;
    private String dataPath;

    public TableCombiner() throws IOException{
        table = TreeBasedTable.create(Comparator.naturalOrder(), idOnFirstCol);
        dataPath = "";
    }

    public TableCombiner(String filesDirectoryPath){
        table = TreeBasedTable.create(Comparator.naturalOrder(), idOnFirstCol);
        dataPath = filesDirectoryPath;
    }

    /**
     * Merge tables and consolidating duplicating column.
     *
     * @param fileNames string array of tables' filenames
     * @return merged guava table
     * @throws IOException if error reading/writing from/to file
     *
     */
    public Table<String, String, String> combineByFileNames
            (String[] fileNames) throws IOException{
        for (String fileName : fileNames){
            switch(FilenameUtils.getExtension(fileName)) {
                case "html":
                    buildTableFromHtml(fileName);
                    break;
                case "csv":
                    buildTableFromCsv(fileName);
                    break;
                // future file format support starts here
                default:
                    break;
            }
        }
        return table;
    }


    /**
     * Build a guava table from the html file supplied
     *
     * ASSUMPTION:
     * 1. table being read must have "directory" as id
     * 2. id column must exist
     * 3. id must not be empty
     *
     * @param fileName of the html file
     * @throws IOException if error reading from file
     *
     */
    private void buildTableFromHtml(String fileName) throws IOException{
        File in = new File(dataPath + fileName);
        Document doc = Jsoup.parse(in, null);
        Elements cols = doc.select("#directory tr th");
        int sizeCols = cols.size();
        int iIndex = -1;
        Map<Integer, String> indexMap = new HashMap<>();

        for (int i = 0; i < sizeCols; i++){
            // identify the index of ID
            if ("ID".equals(cols.get(i).text())){
                iIndex = i;
            }
            indexMap.put(i, cols.get(i).text());

        }

        // ERROR CHECKING: Table without ID
        if (iIndex == -1){
            System.err.println(fileName + ERROR_TABLE_WITHOUT_ID);
            System.exit(1);
        }

        Elements td = doc.select("#directory tr td");
        // ERROR CHECKING: Invalid table
        if (td.size()%sizeCols != 0){
            System.err.println(fileName + ERROR_TABLE_INVALID);
            System.exit(1);
        }
        // populate html table data into table
        for(int i = 0; i < td.size(); i += sizeCols){
            int indexId = sizeCols*(i/sizeCols) + iIndex;
            String id = td.get(indexId).text();
            // ERROR CHECKING: ID not valid
            if (!checkIDValid(id)){
                System.err.println(fileName + ERROR_TABLE_INVALID_ID);
                System.exit(1);
            }
            for (int j = i; j < i + sizeCols; j++) {
                synchronized (this) {
                    if (table.get(id, indexMap.get(j % sizeCols)) == null) {
                        table.put(id, indexMap.get(j % sizeCols),
                                td.get(j).text());
                    } else {
                        // ERROR CHECKING: Table conflict
                        if (!table.get(id, indexMap.get(j % sizeCols))
                                .equals(td.get(j).text())) {
                            System.err.println(fileName + ERROR_TABLE_CONFLICT);
                            System.exit(1);
                        }
                    }
                }
            }
        }

    }

    /**
     * Build a guava table from the csv file supplied
     *
     * ASSUMPTION:
     * 1. table entry being read is seperated by ','
     * 2. id column must exist
     * 3. id must not be empty
     *
     * @param fileName of the csv file
     * @throws IOException if error reading from file
     *
     */
    private void buildTableFromCsv(String fileName) throws IOException{
        CSVReader csvReader = new CSVReader(
                new FileReader(dataPath + fileName),
                ',' , '"' , 0);
        String[] nextLine;
        int sizeCols = -1;
        int iIndex = -1;
        Map<Integer, String> indexMap = new HashMap<>();

        if ((nextLine = csvReader.readNext()) != null){
            sizeCols = nextLine.length;
            for (int i = 0; i < sizeCols; i++){
                // identify the index of ID
                if ("ID".equals(nextLine[i])) {
                    iIndex = i;
                }
                indexMap.put(i, nextLine[i]);
            }
            // ERROR CHECKING: Table without ID
            if (iIndex == -1){
                System.err.println(fileName + ERROR_TABLE_WITHOUT_ID);
                System.exit(1);
            }
        }

        // read csv table row by row into table
        while ((nextLine = csvReader.readNext()) != null) {
            // ERROR CHECKING: Table not valid
            if (nextLine.length != sizeCols) {
                System.err.println(fileName + ERROR_TABLE_INVALID);
                System.exit(1);
            }
            String id = nextLine[iIndex];
            // ERROR CHECKING: ID not valid
            if (!checkIDValid(id)){
                System.err.println(fileName + ERROR_TABLE_INVALID_ID);
                System.exit(1);
            }
            for (int i = 0; i < sizeCols; i++) {
                // Synchronize table operations
                synchronized (this) {
                    if (table.get(id, indexMap.get(i)) == null) {
                        table.put(id, indexMap.get(i),
                                nextLine[i]);
                    } else {
                        // ERROR CHECKING: Table conflict
                        if (!table.get(id, indexMap.get(i)).equals(nextLine[i])) {
                            System.err.println(fileName + ERROR_TABLE_CONFLICT);
                            System.exit(1);
                        }
                    }
                }
            }
        }

    }

    /**
     * Export the table into csv file
     *
     * @param table the guava table to export
     * @throws IOException if error writing to file
     *
     */
    public static void exportTableToCSV(Table<String, String, String> table) throws IOException{
        String outputFileName = "combined.csv";
        CSVWriter writer = new CSVWriter(new FileWriter(outputFileName),
                ',','"');
        Set<String> cols = table.columnKeySet();

        // write column headers to file
        writer.writeNext(cols.toArray(new String[cols.size()]));

        // write row data to file
        for(String id : table.rowKeySet()){
            List<String> rowEntries = new ArrayList<>();
            for(String col : table.columnKeySet()){
                rowEntries.add(table.get(id, col)
                        == null ? "" : table.get(id, col));
            }
            writer.writeNext(rowEntries.toArray(
                        new String[rowEntries.size()]));
        }

        //close the writer
        writer.close();
    }

    private Comparator<String> idOnFirstCol = new Comparator<String>() {
        /** For enhanced readability, make "ID" the smallest value so
         * it appears at first column.
         *
         * @param o1 to compare
         * @param o2 to compare
         * @return <code>1</code> if <>o1</> is larger
         *         <code>0</code> if <>o1</> and <>o2</> are equal
         *         <code>-1</code> if <>o2</> is larger
         */
        @Override
        public int compare(String o1, String o2) {
            if ("ID".equals(o1) && "ID".equals(o2)){
                return 0;
            }else if ("ID".equals(o1)){
                return -1;
            }else if ("ID".equals(o2)){
                return 1;
            }else{
                return o1.compareTo(o2);
            }
        }
    };

    private boolean checkIDValid(String id){
        return !(id == null
                    || "".equals(id.replaceAll("\\s+","")));
    }

}
