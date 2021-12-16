package examples;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class HelloTest {

    @Test
    public void hello() throws IOException, URISyntaxException {
        Hello h = new Hello();
        String hello = h.hello();
        String hellotest = new String(Files.readAllBytes(Paths.get(Thread.currentThread().getContextClassLoader().getResource("hellotest.txt").toURI())));
        Assertions.assertEquals(hellotest, hello);
    }
    
}
