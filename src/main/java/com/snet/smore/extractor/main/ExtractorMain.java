package com.snet.smore.extractor.main;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.extractor.module.DbReadModule;
import com.snet.smore.extractor.module.FileCopyModule;
import com.snet.smore.extractor.module.SocketReceiveModule;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ExtractorMain {
    public static void main(String[] args) {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        Class clazz;
        Object obj;
        Method method;
        String mode = EnvManager.getProperty("extractor.mode").toUpperCase();

        switch (mode) {
            case "FILE":
                obj = new FileCopyModule();
                clazz = FileCopyModule.class;
                break;
            case "SOCKET":
                obj = new SocketReceiveModule();
                clazz = SocketReceiveModule.class;
                break;
            case "RDBMS":
                obj = new DbReadModule();
                clazz = DbReadModule.class;
                break;
            default:
                log.error("Cannot find extractor module class.");
                return;
        }

        String methodName = "execute";
        try {
            method = clazz.getMethod(methodName);
        } catch (NoSuchMethodException e) {
            log.error("Cannot find executable method in class [{}]", clazz.getName());
            return;
        }


        // Main Thread
        Method finalMethod = method;

        Runnable runnable = () -> {
            try {
                finalMethod.invoke(obj);
            } catch (Exception e) {
                log.error("Unknown error occurred. Main thread will be restarted.", e);
            }
        };

        service.scheduleWithFixedDelay(runnable, 5, 1, TimeUnit.SECONDS);
    }

}