package edu.uci.ics.asterix.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;

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
        if(args.length != 5){
            System.out.println("Wrong arguments. Please use it as:\nargs[0]:CC Url" +
                                "\nargs[1]: number of iterations (to run query suite)\n" +
                                "args[2]:Path of the directory,containing query files\n" +
                                "args[3]:Path for the file to save benchmarking stats\n" +
                                "args[4]:Path for the file to dump & save returned resulst for each query (if set it to be null, it means you dont want to save returned results)");
            return;
        }
        
        String ccUrl = args[0];                         //URL of CC for running Asterix instance
        int iteration = Integer.parseInt( args[1] );    //number of times, the whole query suite is gonna be run
        String quDir = args[2];                         //path for directory that contains AQL query files. Each file is should be a complete AQL query
        String statFile = args[3];                      //path for the file that will contain the gathered statistics of benchmarking, at the end
        String resultsDumpFile = args[4];               //path for the file that will contain results, parsed, for each query. If you set it to be "null", it means you do not want to store returned results
        
        File[] qFiles = getQueryFiles(quDir);
        String[] qTexts = null;
        try {
            qTexts = new String[qFiles.length];
            for(int i=0; i<qFiles.length; i++){
                String qPath = qFiles[i].getAbsolutePath();
                BufferedReader in = new BufferedReader(new FileReader(qPath));
                StringBuffer sb = new StringBuffer();
                String str;
                while ((str = in.readLine()) != null) {
                    sb.append(str).append("\n");
                }
                qTexts[i] = sb.toString();
                in.close();
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
                        System.out.println("Results for "+qFiles[ix].getName()+" in iteration "+t);
                        resultsPw.println(parsedResults);
                        resultsPw.flush();
                        System.out.println(parsedResults);
                    }
                        //consuming content of HttpResponse, clearing it and making it ready for next query
                    EntityUtils.consume( response.getEntity() );
                    ix++;
                }
            }
        } catch (Exception e) {
            System.err.println("Problem in query execution or stats dumping");
            e.printStackTrace();
        } finally{
            if(statsPw != null) { statsPw.close(); }
            httpclient.getConnectionManager().shutdown();
            System.out.println("\nAsterix Client finished !");
            
        }
    }
    
    private static File[] getQueryFiles(String dirPath){
         File folder = new File(dirPath);
         File[] listOfFiles = folder.listFiles();
         ArrayList<File> arrayListOfQueries = new ArrayList<File>();
         File[] listOfQueries;
         for (File f : listOfFiles) {
        	 String path = f.getPath();
        	 String[] pathElements = path.split("/");
        	 String fileName = pathElements[pathElements.length - 1];
        	 if (!fileName.startsWith(".")) {
        		 arrayListOfQueries.add(f);
        	 }
         }
         listOfQueries = new File[arrayListOfQueries.size()];
         System.out.println(listOfQueries.length);
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
}
