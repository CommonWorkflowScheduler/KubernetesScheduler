package cws.k8s.scheduler.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DateParser {

    public static Long millisFromString( String date ) {
        if( date == null || date.isEmpty() || date.equals("-") || date.equals("w") ) {
            return null;
        }
        try {
            if (Character.isLetter(date.charAt(0))) {
                // if ls was used, date has the format "Nov 2 08:49:30 2021"
                return new SimpleDateFormat("MMM dd HH:mm:ss yyyy").parse(date).getTime();
            } else {
                // if stat was used, date has the format "2021-11-02 08:49:30.955691861 +0000"
                String[] parts = date.split(" ");
                parts[1] = parts[1].substring(0, 12);
                // parts[1] now has milliseconds as smallest units e.g. "08:49:30.955"
                String shortenedDate = String.join(" ", parts);
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z").parse(shortenedDate).getTime();
            }
        } catch ( Exception e ){
            return null;
        }
    }

    public static FileTime fileTimeFromString(String date ) {
        final Long millisFromString = millisFromString( date );
        return millisFromString == null ? null : FileTime.fromMillis( millisFromString );
    }

}
