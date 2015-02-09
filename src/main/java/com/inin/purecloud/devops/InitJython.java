package com.inin.purecloud.devops;
import jline.ConsoleReader;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hello world!
 *
 */
public class InitJython extends AbstractJythonInit {

    public InitJython(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws ScriptException {
        new InitJython(args).run();
    }

    public void run() throws ScriptException {
        if (args.length > 0) {
            if (args[0].equals("eval"))
                if (args.length > 1)
                    c.exec(args[1]);
                else
                    c.execfile(InitJython.class.getResourceAsStream(
                                    "/Lib/DynamoDbInfo/__init__.py"),
                            "DynamoDbInfo/__init__.py");
            else if (args[0].equals("run"))
                if (args.length > 1)
                    c.execfile(args[1]);
                else {
                    c.execfile(InitJython.class.getResourceAsStream("/Lib/__init__.py"), "__init__.py");
                }
            else if (args[0].equals("script")) {
                String engineName = args[1];
                ScriptEngine eng = new ScriptEngineManager()
                        .getEngineByName(engineName);
                if (eng == null) {
                    throw new NullPointerException("Script Engine '"
                            + engineName + "' not found!");
                }
                eng.put("engine", engineName);
                if (args.length > 2) {
                    System.out.println("result: " + eng.eval(args[2]));
                } else {
                    System.out.println("write your script below; terminate "
                            + "with Ctrl-Z (Windows) or Ctrl-D (Unix) ---");
                    try {
                        System.out.println("result: "
                                + eng.eval(new InputStreamReader(
                                new ConsoleReader().getInput())));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else
                System.out
                        .println("use either eval or run or script as first argument");
        } else
            c.interact();
    }
}