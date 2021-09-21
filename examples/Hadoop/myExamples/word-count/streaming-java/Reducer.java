import java.util.Scanner;

public class Reducer {

	public static void main(String[] args) {
		long count = 0;
		String currentWord = "";
		
		Scanner stdin = new Scanner(System.in);
		while (stdin.hasNextLine()){
			String line = stdin.nextLine().trim();
			String[] field = line.split("\t",0);
			if (field.length > 1) {
				if (field[0].compareTo(currentWord) == 0) 
					count += Integer.parseInt(field[1]);
				else {
					if (count > 0) System.out.println(currentWord + "\t" + count);
					count = Integer.parseInt(field[1]);
					currentWord = field[0];
				}	
			}
		}
		stdin.close();
		System.out.println(currentWord + "\t" + count);
	}
}
