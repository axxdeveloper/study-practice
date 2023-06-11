package examples;

import com.google.common.base.Preconditions;
import java.io.IOException;
import shaded.example.lib.Preconditions31;

public class Hello {

    public String hello() throws IOException {
        Preconditions.checkArgument(true); // This is guava-16 Preconditions
        Preconditions31.validate(true, "s", new Object());
        return "hello";
    }
    
}
