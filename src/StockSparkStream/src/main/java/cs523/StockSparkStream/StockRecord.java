package cs523.StockSparkStream;

import java.util.Arrays;

public class StockRecord {
	private String symbol;
	private String closed;
	private String open;
	private String market;
	
	public StockRecord(String symbol, String closed, String open, String market) {
		super();
		this.symbol = symbol;
		this.closed = closed;
		this.open = open;
		this.market = market;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getClosed() {
		return closed;
	}

	public void setClosed(String closed) {
		this.closed = closed;
	}

	public String getOpen() {
		return open;
	}

	public void setOpen(String open) {
		this.open = open;
	}

	public String getMarket() {
		return market;
	}

	public void setMarket(String market) {
		this.market = market;
	}
	public static StockRecord getStockRecord(String column,String value){
    	  String[] columns = column.split(",");
    	  String[] values = value.split(Global.LinePattern, -1);
    	  int symIndx =  Arrays.asList(columns).indexOf("symbol");
    	  int closedIndx =  Arrays.asList(columns).indexOf("regularMarketPreviousClose");
    	  int openIndx =  Arrays.asList(columns).indexOf("regularMarketOpen");
    	  int marketIndx =  Arrays.asList(columns).indexOf("regularMarketPrice");
    	  
    	  return new StockRecord(values[symIndx],values[closedIndx],values[openIndx],values[marketIndx]);
	}
}
