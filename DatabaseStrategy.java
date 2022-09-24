/* 
Ethan Temple
CSCI 330 W 22
Assignment 2
*/

import java.util.*;

import com.mysql.cj.x.protobuf.MysqlxPrepare.Prepare;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.PrintWriter;
import java.lang.Math;

class DatabaseStrategy
{
   
   static class StockData
   {	   
	   // Create this class which should contain the information  (date, open price, high price, low price, close price) for a particular ticker;
      String TransDate;
      double OpenPrice;
      double HighPrice;
      double LowPrice;
      double ClosePrice;

      StockData(String TransDate, double OpenPrice, double HighPrice, double LowPrice, double ClosePrice)
      {	   
         this.TransDate = TransDate;
         this.OpenPrice = OpenPrice;
         this.HighPrice = HighPrice;
         this.LowPrice = LowPrice;
         this.ClosePrice = ClosePrice;
      }
   }
   
   static Connection conn;
   static final String prompt = "Enter ticker symbol [start/end dates]: ";
   
   public static void main(String[] args) throws Exception
   {
      String paramsFile = "readerparams.txt";
      if (args.length >= 1)
      {
         paramsFile = args[0];
      }
      
      Properties connectprops = new Properties();
      connectprops.load(new FileInputStream(paramsFile));
      try
      {
         Class.forName("com.mysql.jdbc.Driver");
         String dburl = connectprops.getProperty("dburl");
         String username = connectprops.getProperty("user");
         conn = DriverManager.getConnection(dburl, connectprops);
         System.out.printf("Database connection %s %s established.%n", dburl, username);
         
         Scanner in = new Scanner(System.in);
         System.out.print(prompt);
         String input = in.nextLine().trim();
         
         while (input.length() > 0)
         {
            String[] params = input.split("\\s+");
            String ticker = params[0];
            String startdate = null, enddate = null;
            if (getName(ticker))
            {
               if (params.length >= 3)
               {
                  startdate = params[1];
                  enddate = params[2];
               }               
               Deque<StockData> data = getStockData(ticker, startdate, enddate);
               System.out.println();
               System.out.println("Executing investment strategy");
               doStrategy(ticker, data);
            } 
            
            System.out.println();
            System.out.print(prompt);
            input = in.nextLine().trim();
         }

         // Close the database connection
         conn.close();
         in.close();
         System.out.printf("Database connection closed.\n");

      } catch (SQLException ex)
      {
         System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                           ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
      }
   }
   
   static boolean getName(String ticker) throws SQLException
   {
	  // Execute the first query and print the company name of the ticker user provided (e.g., INTC to Intel Corp.) 
     PreparedStatement getName = conn.prepareStatement("select Name from company where Ticker = ?");
     getName.setString(1, ticker);
     ResultSet name = getName.executeQuery();

     if(name.next())
     {
        System.out.printf("%s\n", name.getString(1));
        return true;
     }
     else
     {
        System.out.printf("%s not found in database.\n", ticker);
        return false;
     }
   }

   static Deque<StockData> getStockData(String ticker, String start, String end) throws SQLException
   {	  
	   // Execute the second query which will return stock information of the ticker (descending on the transaction date)  
     
      PreparedStatement getStockInfo;

      if(start == null)
      {
      getStockInfo = conn.prepareStatement("select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice from pricevolume where Ticker = ? order by TransDate DESC");
      getStockInfo.setString(1, ticker);
      }
      else
      {
         getStockInfo = conn.prepareStatement("select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice from pricevolume where Ticker = ? and TransDate >= ? and TransDate <= ? order by TransDate DESC");
         getStockInfo.setString(1, ticker);
         getStockInfo.setString(2, start);
         getStockInfo.setString(3, end);
      }
      ResultSet stocks = getStockInfo.executeQuery();

      Deque<StockData> result = new ArrayDeque<>();

	  // Loop through all the dates of that company (descending order)
			// Find split if there is any (2:1, 3:1, 3:2) and adjust the split accordingly
			// Include the adjusted data to result (which is a Deque)

      //store info about the loop
      double divisor = 1;
      double nextOpen = 0;
      int count = 0;
      int splits = 0;
      if(stocks.next())
	   {
         StockData sd = new StockData(stocks.getString(1), Double.parseDouble(stocks.getString(2)) / divisor, Double.parseDouble(stocks.getString(3)) / divisor, Double.parseDouble(stocks.getString(4)) / divisor, Double.parseDouble(stocks.getString(5)) / divisor);
         result.addFirst(sd);
         nextOpen = sd.OpenPrice;
         count++;
      }
      while(stocks.next())
      {
         double prevClose = Double.parseDouble(stocks.getString(5)) / divisor;
         //check for splits
         if(Math.abs((prevClose/nextOpen) - 2) < 0.2)
         {
            System.out.printf("2:1 split on %s, %-6.2f --> %-6.2f\n", stocks.getString(1), prevClose * divisor, nextOpen * divisor);
            divisor *= 2;
            splits++;
         }
         else if(Math.abs((prevClose/nextOpen) - 3) < 0.3)
         {
            System.out.printf("3:1 split on %s, %-6.2f --> %-6.2f\n", stocks.getString(1), prevClose * divisor, nextOpen * divisor);
            divisor *= 3;
            splits++;
         }
         else if(Math.abs((prevClose/nextOpen) - 1.5) < 0.15)
         {
            System.out.printf("3:2 split on %s, %-6.2f --> %-6.2f\n", stocks.getString(1), prevClose * divisor, nextOpen * divisor);
            divisor *= 1.5;
            splits++;
         }
         //add the data to the deque
         StockData sd = new StockData(stocks.getString(1), Double.parseDouble(stocks.getString(2)) / divisor, Double.parseDouble(stocks.getString(3)) / divisor, Double.parseDouble(stocks.getString(4)) / divisor, Double.parseDouble(stocks.getString(5)) / divisor);
         result.addFirst(sd);
         nextOpen = sd.OpenPrice;
         count++;
      }
      System.out.printf("%d splits in %d trading days\n", splits, count);
      return result;
   }
   
   static void doStrategy(String ticker, Deque<StockData> data)
   {
	   // Apply Steps 2.6 to 2.10 explained in the assignment description 
	   // data (which is a Deque) has all the information (after the split adjustment) you need to apply these steps
      int num = data.size();
      int transactions = 0;
      double cash = 0;
      int shares = 0;
      boolean buy = false;
      Deque<Double> LastDays = new ArrayDeque<>();
      StockData sd = null;

      if(num > 50)
      {
         //get 50 day average for the first 50 trading days
         for(int i = 0; i < 50; i++)
         {
            LastDays.addLast(data.removeFirst().ClosePrice);
         }
         //do strategy
         while(!data.isEmpty())
         {
            double average = getAverage(LastDays);
            sd = data.removeFirst();
            if(buy)
            {
               shares += 100;
               cash -= (100 * sd.OpenPrice);
               cash -= 8;
               buy = false;
               transactions++;
            }
            if(sd.ClosePrice < average && (sd.ClosePrice / sd.OpenPrice) < 0.97000001)
            {
               buy = true;
            }
            else if(shares >= 100 && sd.OpenPrice > average && (sd.OpenPrice / LastDays.peekLast()) > 1.00999999)
            {
               shares -= 100;
               cash += (100 * ((sd.OpenPrice + sd.ClosePrice) / 2));
               cash -= 8;
               transactions++;
            }
            LastDays.removeFirst();
            LastDays.addLast(sd.ClosePrice);
         }
         //sell all remaining shares on last day
         if(shares > 0)
         {
            cash += shares * sd.OpenPrice;
            transactions++;
         }
      }
      System.out.printf("Transactions executed: %d\nNet Cash: %.2f\n", transactions, cash);
   }

   static double getAverage(Deque<Double> a)
   {
      //gets the average of 50 values in a deque
      double sum = 0;
      Iterator<Double> it = a.iterator();
      for(int i = 0; i < 50; i++)
      {
         sum += it.next();
      }
      return sum/50;
   }
}
