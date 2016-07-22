package utils;

import com.google.common.base.Preconditions;

/**
 * Created by lev on 15.07.16.
 */
public class LogParseUtils {
    
    private final static int BYTES_INDEX = 9;

    public static String getIp(String[] data) {
        Preconditions.checkArgument(data != null, "Data is null");
        return data.length > 0 ? data[0] : null;
    }

    public static int getBytes(String [] data) {
        
		Preconditions.checkArgument(data != null, "Data is null");

        int bytes = 0;

		if(data.length >= 9){
            
            try{
                bytes = Integer.parseInt(data[BYTES_INDEX]);
            }catch (NumberFormatException nfe){
                
            }
        }

		return bytes;
    }
}
