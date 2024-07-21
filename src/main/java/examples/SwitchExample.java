package examples;

public class SwitchExample {

    public static void main(String[] args) {
        SwitchExample se = new SwitchExample();
        se.switchExpression();
        se.switchInPrintln();
        se.switchAssignment();
        se.switchYield1();
        se.switchYield2();
        se.switchCanNotContinueForloop();
    }

    private void switchCanNotContinueForloop() {
        x: for (int i = 0; i < 10; i++) {
            int k = switch (i) {
//                case 3 -> {
//                    System.out.println("Three");
//                    continue x; // Error: continue outside of loop
//                }
                default -> {
                    System.out.println("Other");
                    yield 3;
                }
            };
        }
    }

    private void switchYield2() {
        int day = 1;
        String dayType = switch (day) {
            case 1, 2, 3, 4, 5 -> "Weekday";
            case 6, 7 -> "Weekend";
            default -> {
                int rnd = (int) (Math.random() * 10);
                yield "Random " + rnd;
            }
        };
        System.out.println(dayType);
    }

    private void switchYield1() {
        int day = 1;
        String dayType = switch (day) {
            case 1, 2, 3, 4, 5 -> {
                yield "Weekday";
            }
            case 6, 7 -> {
                yield "Weekend";
            }
            default -> {
                yield "Invalid day";
            }
        };
        System.out.println(dayType);
    }

    private void switchAssignment() {
        int day = 1;
        String dayType = switch (day) {
            case 1, 2, 3, 4, 5 -> "Weekday";
            case 6, 7 -> "Weekend";
            default -> "Invalid day";
        };
        System.out.println(dayType);
    }

    private void switchInPrintln() {
        System.out.println(switch (1) {
            case 1 -> "One";
            case 2 -> "Two";
            default -> "Other";
        });
    }

    private void switchExpression() {
        String name = "John";
        String message = switch (name) {
            case "John" -> "Hello John";
            case "Doe" -> "Hello Doe";
            default -> "Hello Stranger";
        };
        System.out.println(message);
    }

}
