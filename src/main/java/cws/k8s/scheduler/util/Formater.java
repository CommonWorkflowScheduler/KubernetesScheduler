package cws.k8s.scheduler.util;

import lombok.NoArgsConstructor;

import java.util.Locale;

@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class Formater {

    public static String formatBytes(Long bytes) {
        if ( bytes == null ) return "-";
        if (bytes < 1024) return formatNumber( bytes, 1, 0 ) + " B";
        if (bytes < 1024 * 1024) return formatNumber( bytes, 1024, 2 ) + " KB";
        if (bytes < 1024 * 1024 * 1024) return formatNumber( bytes, 1024L * 1024, 2 ) + " MB";
        return formatNumber( bytes, 1024L * 1024 * 1024, 2 ) + " GB";
    }

    private static String formatNumber( long number, double divisor, int decimals ) {
        return String.format( Locale.ENGLISH, "%." + decimals + "f", (double) number / divisor );
    }

}
