import java.io.*;
import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.StringTokenizer;

import com.mongodb.*;

public class Parser {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		MongoClient mongoClient = new MongoClient();
		DB db = mongoClient.getDB("FileParser");
		DBCollection accessColl = db.getCollection("accessCollection");
		DBCollection errorColl = db.getCollection("errorCollection");
		DBCollection installColl = db.getCollection("installCollection");
		
		int i = 0;
		int j = 0;
		int k = 0;
		File accessLog = new File("/Users/karanmankodi/Documents/Coursework/BigData/access.log");
		File errorLog = new File("/Users/karanmankodi/Documents/Coursework/BigData/error.log");
		File installLog = new File("/Users/karanmankodi/Documents/Coursework/BigData/install.log");
		@SuppressWarnings("resource")
		Scanner accessScanner = new Scanner(accessLog);
		Scanner errorScanner = new Scanner(errorLog);
		Scanner installScanner = new Scanner(installLog);
		
		if (accessColl.count() < 1)
		{
			while (accessScanner.hasNextLine())
			{
				i++;
				String ipAddress, date, timeZone, method, statusCode, number;
				String line = accessScanner.nextLine().replaceAll("(\\s+-\\s+-\\s+\\[)|(\\]\\s+\")|(\"\\s+)", "###");
				String[] access = line.split("###");
				ipAddress = access[0];
				date = access[1].substring(0 ,access[1].length()-5);
				timeZone = access[1].substring(access[1].length()-5);
				method = access[2];
				statusCode = access[3].substring(0, 3);
				number = access[3].substring(3, access[3].length());
				BasicDBObject accessDB = new BasicDBObject();
				accessDB.put("ipAddress", ipAddress);
				accessDB.put("date", date);
				accessDB.put("timeZone",timeZone);
				accessDB.put("method", method);
				accessDB.put("statusCode", statusCode);
				accessDB.put("number", number);
				accessColl.insert(accessDB, WriteConcern.SAFE);
			}
		}
		
		
		if (errorColl.count() < 1)
		{
			errorScanner.nextLine();
			errorScanner.nextLine();
			errorScanner.nextLine();
			while (errorScanner.hasNextLine())
			{
				j++;
				String date, type, clientIP, message;
				String[] errors;
				String[] notices;
				String currentLine = errorScanner.nextLine();
				String replacedLine  = currentLine.replaceAll("(\\[)|(\\]\\s+\\[)|(\\]\\s+)", "###");
				String[] error = replacedLine.split("###");
				
				if (currentLine.contains("[error]"))
				{
					date = error[1];
					type = error[2];
					clientIP = error[3];
					message = error[4];
					BasicDBObject errorDB = new BasicDBObject();
					errorDB.put("date", date);
					errorDB.put("type",type);
					errorDB.put("clientIP", clientIP);
					errorDB.put("message", message);
					errorColl.insert(errorDB, WriteConcern.SAFE);
				}else if(currentLine.contains("[notice]") || currentLine.contains("[warn]")){
					date = error[1];
					type = error[2];
					message = error[3];
					BasicDBObject errorDB = new BasicDBObject();
					errorDB.put("date", date);
					errorDB.put("type",type);
					errorDB.put("message", message);
					errorColl.insert(errorDB, WriteConcern.SAFE);
				}
				
			}
		}
		
		if (installColl.count() < 1)
		{
			installScanner.nextLine();
			installScanner.nextLine();
			installScanner.nextLine();
			installScanner.nextLine();
			installScanner.nextLine();
			installScanner.nextLine();
			installScanner.nextLine();
			while (installScanner.hasNextLine())
			{
				k++;
				String action, path;;
				String firstPart, colon;
				StringTokenizer installString = new StringTokenizer(installScanner.nextLine(), ":", true);
				firstPart = installString.nextToken(); 
				colon = installString.nextToken();
				action = firstPart.substring(0, firstPart.length()-2);
				path = firstPart.substring(firstPart.length()-2) + colon +installString.nextToken();
				BasicDBObject installDB = new BasicDBObject();
				installDB.put("action", action);
				installDB.put("path",path);
				installColl.insert(installDB, WriteConcern.SAFE);
			}
		}
		
		
		
		
		System.out.println("Which collection do you want to read? Type 1 for access, 2 for error, 3 for install.");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int userInput = Integer.parseInt(br.readLine());
		if (userInput == 1)
		{
			System.out.println("Access selected");
			System.out.println("Select search criteria");
			System.out.println("1 for IP, 2 for date, 3 for timezone, 4 for method, 5 for statusCode, 6 for number");
			int accessSearchCriteria = Integer.parseInt(br.readLine());
			System.out.println("Enter search string");
			String accessSearchString = br.readLine();
			BasicDBObject accessDB = new BasicDBObject();
			if (accessSearchCriteria == 1)
			{
				accessDB.put("ipAddress", accessSearchString);
				System.out.println(accessColl.find(accessDB));
			} else if (accessSearchCriteria == 2)
			{
				accessDB.put("date", accessSearchString);
				System.out.println(accessColl.find(accessDB));
			} else if (accessSearchCriteria == 3)
			{
				accessDB.put("timeZone", accessSearchString);
				System.out.println(accessColl.find(accessDB));
			} else if (accessSearchCriteria == 4)
			{
				accessDB.put("method", accessSearchString);
				System.out.println(accessColl.find(accessDB));
			} else if (accessSearchCriteria == 5)
			{
				accessDB.put("statusCode", accessSearchString);
				System.out.println(accessColl.find(accessDB));
			} else if (accessSearchCriteria == 6)
			{
				accessDB.put("number", accessSearchString);
				System.out.println(accessColl.find(accessDB));
			}
			
		} else if (userInput == 2)
		{
			System.out.println("Error selected");
			System.out.println("Select search criteria");
			System.out.println("1 for date, 2 for type, 3 for clientIP, 4 for message");
			int accessSearchCriteria = Integer.parseInt(br.readLine());
			System.out.println("Enter search string");
			String accessSearchString = br.readLine();
			BasicDBObject errorDB = new BasicDBObject();
			if (accessSearchCriteria == 1)
			{
				errorDB.put("date", accessSearchString);
				System.out.println(accessColl.find(errorDB));
			} else if (accessSearchCriteria == 2)
			{
				errorDB.put("type", accessSearchString);
				System.out.println(accessColl.find(errorDB));
			} else if (accessSearchCriteria == 3)
			{
				errorDB.put("clientIP", accessSearchString);
				System.out.println(accessColl.find(errorDB));
			} else if (accessSearchCriteria == 4)
			{
				errorDB.put("message", accessSearchString);
				System.out.println(accessColl.find(errorDB));
			}
		} else if (userInput == 3)
		{
			System.out.println("Install selected");
			System.out.println("Select search criteria");
			System.out.println("1 for action, 2 for path");
			int accessSearchCriteria = Integer.parseInt(br.readLine());
			System.out.println("Enter search string");
			String accessSearchString = br.readLine();
			BasicDBObject installDB = new BasicDBObject();
			if (accessSearchCriteria == 1)
			{
				installDB.put("action", accessSearchString);
				System.out.println(accessColl.find(installDB));
			} else if (accessSearchCriteria == 2)
			{
				installDB.put("path", accessSearchString);
				System.out.println(accessColl.find(installDB));
			}
		} else 
		{
			System.out.println(userInput);
		}
		
		
		accessScanner.close();
		installScanner.close();
		errorScanner.close();
		
		mongoClient.close();
	}

	
}
