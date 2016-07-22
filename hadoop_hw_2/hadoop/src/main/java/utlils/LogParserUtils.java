package utlils;

import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Lev_Khacheresiantc on 7/22/2016.
 */
public class LogParserUtils {
    private static String LOG_REGEXP = "";
    private static String SPACE = "\\s+";
    private static String TAB = "\\t";

    static {
        LOG_REGEXP = LOG_REGEXP + "^(\\S+)" + TAB; // Bid ID (*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Timestamp
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Log type
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // iPinyou ID(*)
        LOG_REGEXP = LOG_REGEXP + "(?<userAgent>\\w.+)" + TAB; // User-Agent
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // IP(*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Region
        LOG_REGEXP = LOG_REGEXP + "(?<city>\\S+)" + TAB; // City
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Ad Exchange(*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Domain(*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // URL(*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Anonymous URL ID
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Ad slot ID
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Ad slot width
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // AAd slot height
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Ad slot visibility
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Ad slot format
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Ad slot floor price
                                                  // (RMB/CPM) (*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Creative ID
        LOG_REGEXP = LOG_REGEXP + "(?<bidPrice>\\S+)" + TAB; // Bidding price
                                                             // (RMB/CPM) (*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Paying price (RMB/CPM) (*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Key page URL (*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)" + TAB; // Advertiser ID(*)
        LOG_REGEXP = LOG_REGEXP + "(\\S+)"; // User Tags(*)
    }

    public static Pattern LOG_PATTERN = Pattern.compile(LOG_REGEXP);

    public static String operationalSystem(String line) {
        OperatingSystem operatingSystem =
            UserAgent.parseUserAgentString(line).getOperatingSystem();

        return operatingSystem.getGroup().getName();

    }
    public interface Groups {
        String BID_PRICE = "bidPrice";
        String USER_AGENT = "userAgent";
        String CITY = "city";
    }

}
