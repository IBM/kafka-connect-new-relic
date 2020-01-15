package com.ibm.connect.newrelic;

public class Utils {

    public static boolean empty(final String s) {
        return s == null || s.trim().isEmpty();
    }

    public static String underscoresForPeriods(String periods) {
        return periods.replaceAll("\\.", "__");
    }

	public static Object sanitize(String k, Object v) {
		return k.contains("apikey") ? "<***>" : v;
    }
    
    public static String cleanTextContent(String text) 
    {
        // strips off all non-ASCII characters
        text = text.replaceAll("[^\\x00-\\x7F]", "");
     
        // erases all the ASCII control characters
        text = text.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
         
        // removes non-printable characters from Unicode
        text = text.replaceAll("\\p{C}", "");
     
        return text.trim();
    }
}