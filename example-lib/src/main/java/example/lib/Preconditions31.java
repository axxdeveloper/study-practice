package example.lib;

import com.google.common.base.Preconditions;

public class Preconditions31 {

    public static void validate(boolean b, String s, Object o) {
        Preconditions.checkArgument(b, s, o);
    }


}
