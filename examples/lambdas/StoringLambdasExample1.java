/**
 *  From https://www.w3schools.com/java/java_lambda.asp
 *  Modified by amit
 */
interface StringFunction {
  String run(String str);
}

public class StoringLambdasExample1 {

  public static void main(String[] args) {
    StringFunction exclaim = (s) -> s + "!";
    StringFunction ask = (s) -> s + "?";
    printFormatted("Hello", exclaim);
    printFormatted("Hello", ask);
  }

  public static void printFormatted(String str, StringFunction format) {
    String result = format.run(str);
    System.out.println(result);
  }
}
