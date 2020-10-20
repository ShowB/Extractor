package com.snet.smore.extractor.main;

import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.domain.Agent;
import com.snet.smore.common.util.AgentUtil;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.extractor.module.DbReadModule;
import com.snet.smore.extractor.module.FileCopyModule;
import com.snet.smore.extractor.module.RabbitMQReceiveModule;
import com.snet.smore.extractor.module.SocketReceiveModule;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ExtractorMain {
    private String agentType = Constant.AGENT_TYPE_EXTRACTOR;
    private String agentName = EnvManager.getProperty("extractor.name");

    private boolean isRequiredPropertiesUpdate = true;
    private boolean isFirstRun = true;

    private ScheduledExecutorService mainService = Executors.newSingleThreadScheduledExecutor();
    private ScheduledExecutorService monitorForSocketService = Executors.newSingleThreadScheduledExecutor();


    private String mode = "";

    private Class clazz;
    private Object instance;

    private static Boolean isRunningByMonitor = false;

    public static Boolean getIsRunningByMonitor() {
        return isRunningByMonitor;
    }

    public static void setIsRunningByMonitor(boolean bool) {
        isRunningByMonitor = bool;
    }

    public static void main(String[] args) {
        ExtractorMain main = new ExtractorMain();

        main.mainService.scheduleWithFixedDelay(main::runAgent, 3, 1, TimeUnit.SECONDS);
        main.monitorForSocketService.scheduleWithFixedDelay(main::monitorForSocket, 3, 3, TimeUnit.SECONDS);
    }

    private void runAgent() {
        try {
            final Agent agent = AgentUtil.getAgent(agentType, agentName);

            if (!Constant.YN_Y.equalsIgnoreCase(agent.getUseYn()))
                return;

            isRequiredPropertiesUpdate = Constant.YN_Y.equalsIgnoreCase(agent.getChangeYn());

            if (isRequiredPropertiesUpdate || isFirstRun) {
                clazz = null;
                instance = null;

                EnvManager.reload();
                agentName = EnvManager.getProperty("extractor.name");

                log.info("Environment has successfully reloaded.");

                if ("Y".equalsIgnoreCase(agent.getChangeYn()))
                    AgentUtil.setChangeYn(agentType, agentName, Constant.YN_N);

                isRequiredPropertiesUpdate = false;
                setIsRunningByMonitor(false);
            }

            mode = EnvManager.getProperty("extractor.mode").toUpperCase();

            if (clazz == null) {
                if ("FILE".equalsIgnoreCase(mode)) {
                    clazz = FileCopyModule.class;
                } else if ("SOCKET".equalsIgnoreCase(mode)) {
                    clazz = SocketReceiveModule.class;
                } else if ("RDBMS".equalsIgnoreCase(mode)) {
                    clazz = DbReadModule.class;
                } else if ("RABBITMQ".equalsIgnoreCase(mode)) {
                    clazz = RabbitMQReceiveModule.class;
                } else {
                    log.error("Cannot find extractor module class.");
                    return;
                }
            }

            if (instance == null) {
                instance = clazz.newInstance();
            }

            if ("SOCKET".equalsIgnoreCase(mode) || "RABBITMQ".equalsIgnoreCase(mode))
                setIsRunningByMonitor(true);

            clazz.getMethod("execute").invoke(instance);

            if (isFirstRun)
                isFirstRun = false;

        } catch (Exception e) {
            log.error("An error occurred while thread processing. It will be restarted : {}", e.getMessage());
        }
    }

    private void monitorForSocket() {
        try {
            if (getIsRunningByMonitor()) {
                final Agent agent = AgentUtil.getAgent(agentType, agentName);
                if (Constant.YN_N.equalsIgnoreCase(agent.getUseYn())) {
                    setIsRunningByMonitor(false);

                    if ("SOCKET".equalsIgnoreCase(mode)) {
                        // accept() 함수의 lock을 해제하기 위해 local 접속 생성
                        try {
                            Socket tempSocket = new Socket("127.0.0.1"
                                    , EnvManager.getProperty("extractor.source.socket.port", 50031));
                            tempSocket.close();
                        } catch (IOException e) {
                            log.info("Socket server is already closed.");
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("An error occurred while thread processing. It will be restarted : {}", e.getMessage());
        }
    }

}