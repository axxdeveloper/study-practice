package examples;

public class SwitchMain {

    public static void main(String[] args) {
        String s = "abc";
        switch (s) {
            case "A" -> System.out.println("A");
            case "B" -> System.out.println("B");
            case "abc" -> System.out.println("ABC");
        }
    }

}
