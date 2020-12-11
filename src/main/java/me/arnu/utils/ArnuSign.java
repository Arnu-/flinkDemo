package me.arnu.utils;

import java.io.IOException;
import java.io.OutputStream;

public class ArnuSign {
    public static String GetArnuSign() {
        return "\n#     __\n" +
                "#    /  |  ____ ___  _  \n" +
                "#   / / | / __//   // / /\n" +
                "#  /_/`_|/_/  / /_//___/\n";
    }

    public static void printSign() {
        printSign(null);
    }

    public static void printSign(OutputStream output) {
        if (output == null) {
            output = System.out;
        }
        try {
            output.write(GetArnuSign().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
