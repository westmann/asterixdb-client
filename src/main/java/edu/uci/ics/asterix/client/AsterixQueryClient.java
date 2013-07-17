package edu.uci.ics.asterix.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class AsterixQueryClient {
    public static void main(String args[]){
        if(args.length != 5 && args.length != 7){
            System.out.println("Wrong argu;ments. If validation of results is not desired, please use it as:\nargs[0]:CC Url" +
                                "\nargs[1]: number of iterations (to run query suite)\n" +
                                "args[2]:Path of the directory,containing query files\n" +
                                "args[3]:Path for the file to save benchmarking stats\n" +
                                "args[4]:Path for the file to dump & save returned resulst for each query (if set it to be null, it means you dont want to save returned results)" +
            					"\n If validation of results is desired, please add in addition to the above arguments:\n" +
                                "arg[5]:-v" +
            					"arg[6]:Path for the directory conatining correct query results.  Results files should\n" +
                                "       contain files with the same name, minus the extension, as the queries.");
            return;
        }
        
        String ccUrl = args[0];                         //URL of CC for running Asterix instance
        int iteration = Integer.parseInt( args[1] );    //number of times, the whole query suite is gonna be run
        String quDir = args[2];                         //path for directory that contains AQL query files. Each file is should be a complete AQL query
        String statFile = args[3];                      //path for the file that will contain the gathered statistics of benchmarking, at the end
        String resultsDumpFile = args[4];               //path for the file that will contain results, parsed, for each query. If you set it to be "null", it means you do not want to store returned results
        
        boolean optionVSupplied = false;                //boolean indicating that the -v option was given by the user.  The program should validate the results if this is set to true.
        String answersDir = null;								//path for directory that contains the correct AQL query results files.
        boolean allResultsValid = true;                  //boolean indicating if all results are valid for all iterations.
        boolean missingAnswersFiles = false;			//boolean indicating that not all queries had corresponding correct results files.
        if (args.length == 7) {
        	if (!args[5].equals("-v")) {
        		System.out.println("Invalid option for args[5].  Should have been '-v', but was '" + args[5] + "'");
        		return;
        	}
        	optionVSupplied = true;
        	answersDir = args[6];
        }
        
        File[] qFiles = getFiles(quDir);
        String[] qTexts = null;
        try {
            qTexts = new String[qFiles.length];
            for(int i=0; i<qFiles.length; i++){
            	qTexts[i] = getFileText(qFiles[i]);
            }
        } catch (Exception e) {
            System.err.println("Problem in reading and loading query files");
            e.printStackTrace();
        }
        
        URIBuilder builder = null;
        HttpClient httpclient = null;
        HttpGet httpGet = null;
        PrintWriter statsPw = null;
        PrintWriter resultsPw = null;
        try {
            builder = new URIBuilder("http://"+ccUrl+":19002/query");;
            httpclient = new DefaultHttpClient();
            httpGet = new HttpGet();
            statsPw = new PrintWriter(statFile);
            statsPw.println("Iteration\tQuery-File\tTime(ms)\tResults-count");
            if(resultsDumpFile != null){
                resultsPw = new PrintWriter(resultsDumpFile);
            }
        } catch (Exception e) {
            System.err.println("Problem in setting request sending utils and/or stats writer");
            e.printStackTrace();
        }
        
        try {
            for(int t=0; t<iteration; t++){
                int ix = 0;
                System.out.println("Running queries in iteration "+t);
                for(String nxq : qTexts){
                        //Preparing the query
                    builder.setParameter("query", nxq);
                    URI uri = builder.build();
                    httpGet.setURI(uri);
                        //Running & Measuring Time 
                    long s = System.currentTimeMillis();
                    HttpResponse response = httpclient.execute(httpGet);
                    long e = System.currentTimeMillis();
                    long duration = e-s;
                        //Parsing the results & extracting cardinality & returned records
                    HttpEntity entity = response.getEntity();
                    String content = EntityUtils.toString(entity);
                    int resSize = getResultsSize(content);
                    if(resSize < 0){
                        System.err.println(qFiles[ix].getName()+" in iterattion "+t+" returned invalid results.\n");
                    }
                    statsPw.println(t+"\t"+qFiles[ix].getName()+"\t"+duration+"\t"+resSize);    //saving time/cardinality stats in stats file
                    if(resultsPw != null){  //saving parsed results (all the returned records for the query) in results file
                        String parsedResults = parseResults(content);
                        resultsPw.println("Results for "+qFiles[ix].getName()+" in iteration "+t);
                        resultsPw.println(parsedResults);
                        resultsPw.flush();
                    }
                    
                    if (optionVSupplied == true) {  //checking if query results match correct results
                    	File answerFile = getAnswerFileForQuery(qFiles[ix], answersDir);
                    	if (answerFile == null) {
                    		missingAnswersFiles = true;
                    		System.out.println("Correct results for " + qFiles[ix].getName() + " not in directory " + answersDir);
                    	} else if (!resultsAreValid(parseResults(content), getFileText(getAnswerFileForQuery(qFiles[ix], answersDir)))) {
                    		allResultsValid = false;
                    		System.out.println("Results for " + qFiles[ix].getName() + " in iteration " + t + " are INVALID.");
                    	} else {
                    		System.out.println("Results for " + qFiles[ix].getName() + " in iteration " + t + " are VALID.");
                    	}
                    }
                    
                        //consuming content of HttpResponse, clearing it and making it ready for next query
                    EntityUtils.consume( response.getEntity() );
                    ix++;
                }
                
                if (optionVSupplied && allResultsValid == true) {
                	if (allResultsValid == true) {
                		if (missingAnswersFiles == false) {
                			System.out.println("\nAll results are VALID");
                		} else {
                			System.out.println("\nSome correct results files missing.\nAll results for queries with correct results files are VALID.");
                		}
                	} else {
                		System.out.println("\nSome files are INVALID");
                		if (missingAnswersFiles == true) {
                			System.out.println("Some correct results files are missing");
                		}
                	}
                }
                
            }
        } catch (Exception e) {
            System.err.println("Problem in query execution, stats dumping, or retrieving correct results files.");
            e.printStackTrace();
        } finally{
            if(statsPw != null) { statsPw.close(); }
            httpclient.getConnectionManager().shutdown();
            System.out.println("\nAsterix Client finished !");
            
        }
    }
    
    private static File[] getFiles(String dirPath){
         File folder = new File(dirPath);
         File[] listOfFiles = folder.listFiles();
         ArrayList<File> arrayListOfQueries = new ArrayList<File>();
         File[] listOfQueries;
         for (File f : listOfFiles) {
        	 String fileName = f.getName();
        	 if (!fileName.startsWith(".")) {
        		 arrayListOfQueries.add(f);
        	 }
         }
         listOfQueries = new File[arrayListOfQueries.size()];
         for (int i = 0; i < arrayListOfQueries.size(); i++) {
        	 listOfQueries[i] = arrayListOfQueries.get(i);
         }
         return listOfQueries;
    }
    
    private static int getResultsSize(String content){
        int count = 0;
        try {
            JsonFactory jsonFactory = new JsonFactory();
            JsonParser resultParser = jsonFactory.createParser(content);
            while (resultParser.nextToken() == JsonToken.START_OBJECT) {
                while (resultParser.nextToken() != JsonToken.END_OBJECT) {
                    String key = resultParser.getCurrentName();
                    if (key.equals("results")) {
                        // Start of array.
                        resultParser.nextToken();
                        while (resultParser.nextToken() != JsonToken.END_ARRAY) {
                            resultParser.getValueAsString();
                            count++;
                        }
                    } else {
                        String summary = resultParser.getValueAsString();
                        if (key.equals("summary")) {
                            System.err.println("Exception - Could not find results key in the JSON Object\n"+summary+"\npartial count:\t"+count);
                        }
                        return -1;
                    }
                }
            }
            
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }
    
    private static String parseResults(String content){
        StringBuffer sb = new StringBuffer();
        try {
            JsonFactory jsonFactory = new JsonFactory();
            JsonParser resultParser = jsonFactory.createParser(content);
            while (resultParser.nextToken() == JsonToken.START_OBJECT) {
                while (resultParser.nextToken() != JsonToken.END_OBJECT) {
                    String key = resultParser.getCurrentName();
                    if (key.equals("results")) {
                        // Start of results array
                        resultParser.nextToken();
                        while (resultParser.nextToken() != JsonToken.END_ARRAY) {
                            String record = resultParser.getValueAsString();
                            sb.append(record);
                        }
                    } else {
                        String summary = resultParser.getValueAsString();
                        if (key.equals("summary")) {
                            sb.append("Exception - Could not find results key in the JSON Object\n");
                            sb.append(summary);
                            return (sb.toString());
                        }
                    }
                }
            }
            
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (sb.toString());
    }
    
    /**
     * 
     * @param f: the file which you want to get text from
     * @return the text of the file as a string
     * @throws Exception
     */
    private static String getFileText(File f) throws Exception {
    	
    	Path fPath = f.toPath();
    	if (Files.isSymbolicLink(fPath)) {
    		String absPath = generateAbsolutePathString(f.getAbsolutePath(), Files.readSymbolicLink(fPath).toString());
    		f = new File(absPath);
    	}
    	
        BufferedReader in = new BufferedReader(new FileReader(f));
        StringBuffer sb = new StringBuffer();
        String str;
        while ((str = in.readLine()) != null) {
            sb.append(str).append("\n");
        }
        in.close();
        return sb.toString();
    }
    
    /**
     * 
     * @param f: the file that contains the query you want the correct results for
     * @param answersDir: the directory that contains the files with the correct query results
     * @return the file that contains the correct results for the query in File f
     * @throws Exception
     */
    private static File getAnswerFileForQuery(File f, String answersDir) {
    	String fNameWithoutAQLExt = f.getName().split(".aql")[0];
    	File[] aFiles = getFiles(answersDir);
    	for (File a : aFiles) {
    		if (Pattern.matches("^" + fNameWithoutAQLExt + "[^~]*$", a.getName()) && !(Pattern.matches(".*\\.swp$", a.getName()))) {
    			return a;
    		}
    	}
    	return null;
    }
    
    /**
     * 
     * @param pathToSymLinkFile: Path to a symbolic link file.
     * @param symLinkPath: The path in the symbolic link file.
     * @return The path in the symbolic link file as an absolute path.
     */
    private static String generateAbsolutePathString(String pathToSymLinkFile, String symLinkPath) {
    	if (!symLinkPath.startsWith("/")) {
			String[] pathToSymLinkArray  = pathToSymLinkFile.split("/");
			String[] symLinkPathArray = symLinkPath.split("/");
			int absPathPrefixIndex = pathToSymLinkArray.length - 1;
			int absPathSuffixIndex = 0;
			for (String s : symLinkPathArray) {
				if (s.equals("..")) {
					absPathPrefixIndex--;
				} else if (!s.equals(".")) {
					break;
				}
				absPathSuffixIndex++;
			}
			
			String absPath = new String("");
			for (int i = 0; i < absPathPrefixIndex; i++) {
				absPath += pathToSymLinkArray[i] + "/";
			}
			
			for (int i = absPathSuffixIndex; i < (symLinkPathArray.length - 1); i++) {
				absPath += symLinkPathArray[i] + "/";
			}
			
			absPath += symLinkPathArray[symLinkPathArray.length - 1];
			return absPath;
		} else {
			return symLinkPath;
		}
    }
    
    /**
     * 
     * @param results1: String representation of results.
     * @param results2: String representation of results
     * @return Return true if and only if results1 and results2
     * 		   are the same within a certain numeric tolerance.
     */
    private static boolean resultsAreValid(String results1, String results2) {
    	String[] r1Lines = results1.split(System.getProperty("line.separator"));
    	String[] r2Lines = results2.split(System.getProperty("line.separator"));
    	if (r1Lines.length != r2Lines.length) {
    		return false;
    	}
    	for (int i = 0; i < r1Lines.length; i++) {
    		if (!linesAreEquivalent(r1Lines[i], r2Lines[i])) {
    			return false;
    		}
    	}
    	return true;
    }
    
    
    private static boolean linesAreEquivalent(String l1, String l2) {
    	String[] l1Words = l1.split(" ");
		String[] l2Words = l2.split(" ");
		if (l1Words.length != l2Words.length) {
			return false;
		}
		
		for (int i = 0; i < l1Words.length; i++) {
			String l1Word = l1Words[i];
			String l2Word = l2Words[i];
			int tolerance = 3;
			if (Pattern.matches("^[0-9]*\\.[0-9]+d,?$", l1Word)) {
				if (!Pattern.matches("^[0-9]*\\.[0-9]+d,?$", l2Word)) {
					return false;
				}
			
				if (!equalWithinTolerance(l1Word, l2Word, tolerance)) {
					return false;
				}
				
				continue;		
			}
		
			if (Pattern.matches("^[0-9]*\\.[0-9]+E[0-9]+d,?$", l1Word)) {
				if (!Pattern.matches("^[0-9]*\\.[0-9]+E[0-9]+d,?$", l2Word)) {
					return false;
				}
				
				if (!equalWithinTolerance(l1Word, l2Word, tolerance)) {
					return false;
				}
				
				continue;
			}
			
			if (!l1Word.equals(l2Word)) {
				return false;
			}
		}
		return true;
    }
    
    /**
     * 
     * @param n1: a number as a string with a "d" suffix
     * @param n2: a number as a string with a "d" suffix
     * @param toleranceFactor: the tolerance for min(last significant digit position, 4)
     * @return Return true if an only if n1 and n2 are the same within the tolerance. 
     */
    private static boolean equalWithinTolerance(String n1, String n2, int toleranceFactor) {
    	n1 = n1.split(",")[0];
    	n2 = n2.split(",")[0];
		int n1DotPos = n1.indexOf(".");
		int n2DotPos = n2.indexOf(".");
		if (n1DotPos != n2DotPos) {
			return false;
		}
		int minDigitsAfterDecimal = Math.min(n1.length() - 2, n2.length() - 2)  - n1DotPos;
		int toleranceDigit = Math.min(minDigitsAfterDecimal, 4);
		double tolerance = toleranceFactor * Math.pow(10, -toleranceDigit);
		Double n1Double = new Double(n1.substring(0, n1.length() - 1));
		Double n2Double = new Double(n2.substring(0, n2.length() - 1));
		boolean differenceWithinTolerance =  (Math.abs(n1Double - n2Double) <= tolerance);
		/*
		if (!differenceWithinTolerance) {
			System.out.println("difference :" + Math.abs(n1Double - n2Double));
			System.out.println("tolerance :" + (toleranceFactor * tolerance));
		}
		*/
		return differenceWithinTolerance;
    }
    
}
