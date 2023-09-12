import java.util.Scanner;

public class Mapper {

	public static void main(String[] args) {
		Scanner stdin = new Scanner(System.in);
		while (stdin.hasNextLine()){
			String line = stdin.nextLine().trim();
			String[] words = line.split("[ \t\n\r]");
			for (String word: words) {
				System.out.println(word + "\t" + "1");
			}
		}
		stdin.close();
	}
}
