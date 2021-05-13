import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Utils {

	/**Finds all of the files within all of the sub-folders of a given folder. <br>
	 * Recursively applies itself to each sub-folder found and adds the result to the main list.*/
	public static ArrayList<File> getAllFilesInFolder(File folder) {
		/**Stores the files in the folder.*/
		ArrayList<File> files = new ArrayList<File>();
		
		/**Checks if the file is a folder before beginning.*/
		if(folder.isDirectory()) {
			/**Iterates over each file in the current directory.*/
			for(File file : folder.listFiles()) {
				/**Checks if the file is a directory.*/
				if(file.isDirectory()) {
					/**Adds the files within to the list recursively.*/
					files.addAll(getAllFilesInFolder(file));
				} else {
					files.add(file);
				}
			}
		} else throw new IllegalArgumentException("\"" + folder.getName() + "\" is not a folder!");
		
		/**Returns the completed list.*/
		return files;
	}
	
	/**Executes the given operation with a given timeout. Returns true if completed within the timeout.*/
	public static boolean executeWithTimeout(long timeout, Runnable operation) throws InterruptedException {
		ExecutorService timeoutService = Executors.newSingleThreadExecutor();
		timeoutService.execute(operation);
		timeoutService.shutdown();
		return timeoutService.awaitTermination(timeout, TimeUnit.MILLISECONDS);
	}
}