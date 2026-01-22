package cs523.StockSparkStream;

public class Utilities {
	public static String removeDoubleQuote(String s){
		if(s.length() >= 2)
			return s.substring(1, s.length() -1);
		return s;
	}
}
