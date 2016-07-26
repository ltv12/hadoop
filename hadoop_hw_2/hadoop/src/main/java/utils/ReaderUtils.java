package utils;

import IdAndCount.IdAndCount;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Created by Lev_Khacheresiantc on 7/25/2016.
 */
public class ReaderUtils {
    public static IdAndCount readNextObject(BufferedReader reader)
        throws IOException {
        String line = "";
        try {
            if (reader.ready()) {
                line = reader.readLine();
            }
            if (StringUtils.isEmpty(line)) {
                return null;
            } else {
                String[] data = line.split("\\s+");
                return new IdAndCount(data[0], Integer.parseInt(data[1]));
            }
        } catch (NumberFormatException nfe) {
            System.out.println(line + " --> from " + reader);
            throw nfe;
        }

    }
}
