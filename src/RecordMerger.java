import com.google.common.collect.Table;

public class RecordMerger {

	/**
	 * Entry point of this test.
	 *
	 * @param args command line arguments: first.html and second.csv.
	 * @throws Exception bad things had happened.
	 */
	public static void main(final String[] args) throws Exception {

		if (args.length == 0) {
			System.err.println("Usage: java RecordMerger file1 [ file2 [...] ]");
			System.exit(1);
		}
		// your code starts here.
		TableCombiner tableCombiner
				= new TableCombiner("data/");
		Table<String, String, String> combinedTable
				= tableCombiner.combineByFileNames(args);
		TableCombiner.exportTableToCSV(combinedTable);

	}






}
