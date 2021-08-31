
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

/**
 * A solution to the word count problem using sequential techniques.
 * @author amit
 *
 */
public class WordCount 
{
	private HashMap<String, Integer> dictionary = new HashMap<String, Integer>();
	
	/**
	 * Constructor: Process all files in the given folder into a dictionary.
	 * @param inputFolder the folder containing the text files
	 */
	public WordCount(File inputFolder) 
	{
		for (File f : inputFolder.listFiles()) {
			System.err.println("Processing file: " + f);
			processFile(f);
		}
	}
	
	/**
	 * Process the words in one file and add them to the dictionary (incrementing the count).
	 * @param file
	 */
	private void processFile(File file) 
	{
		try {
			Scanner scan = new Scanner(file);
			while (scan.hasNext()) {
				String word = scan.next();
				if (dictionary.containsKey(word)) {
					Integer count = dictionary.get(word);
					dictionary.replace(word, new Integer(count.intValue() + 1));
				} else {
					dictionary.put(word, 1);
				}
			}
			scan.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}	
	}
	
	/**
	 * Print all words and their counts in the dictionary.
	 */
	private void printDictionary( ) {
		for (String word: dictionary.keySet()) {
			System.out.println(word + " " + dictionary.get(word));
		}
	}

	public static void main(String[] args) 
	{
		String input = args[0];
		File inputFolder = new File(input);
		WordCount processor = new WordCount(inputFolder);
		processor.printDictionary();
	}

}
