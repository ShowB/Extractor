package com.snet.smore.extractor.main;

import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.domain.Agent;
import com.snet.smore.common.util.AgentUtil;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.extractor.module.DbReadModule;
import com.snet.smore.extractor.module.FileCopyModule;
import com.snet.smore.extractor.module.SocketReceiveModule;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ExtractorMain {
    private static String agentType = Constant.AGENT_TYPE_EXTRACTOR;
    private static String agentName = EnvManager.getProperty("extractor.name");

    private static boolean isRequiredPropertiesUpdate = true;
    private static boolean isFirstRun = true;

    private static ScheduledExecutorService mainService = Executors.newSingleThreadScheduledExecutor();
    private static ScheduledExecutorService monitorForSocketService = Executors.newSingleThreadScheduledExecutor();

    private static Boolean isRunningByMonitor = false;

    public static Boolean getIsRunningByMonitor() {
        synchronized (isRunningByMonitor) {
            return isRunningByMonitor;
        }
    }

    public static void setIsRunningByMonitor(boolean bool) {
        synchronized (isRunningByMonitor) {
            isRunningByMonitor = bool;
        }
    }

    public static void main(String[] args) {
        mainService.scheduleWithFixedDelay(ExtractorMain::runAgent, 3, 1, TimeUnit.SECONDS);
        monitorForSocketService.scheduleWithFixedDelay(ExtractorMain::monitorForSocket, 3, 3, TimeUnit.SECONDS);
    }

    private static void runAgent() {
        try {
            final Agent agent = AgentUtil.getAgent(agentType, agentName);

            if (!Constant.YN_Y.equalsIgnoreCase(agent.getUseYn()))
                return;

            isRequiredPropertiesUpdate = Constant.YN_Y.equalsIgnoreCase(agent.getChangeYn());

            if (isRequiredPropertiesUpdate || isFirstRun) {
                EnvManager.reload();
                agentName = EnvManager.getProperty("extractor.name");

                log.info("Environment has successfully reloaded.");

                if ("Y".equalsIgnoreCase(agent.getChangeYn()))
                    AgentUtil.setChangeYn(agentType, agentName, Constant.YN_N);

                isRequiredPropertiesUpdate = false;
            }

            String mode = EnvManager.getProperty("extractor.mode").toUpperCase();

            switch (mode) {
                case "FILE":
                    new FileCopyModule().execute();
                    break;
                case "SOCKET":
                    setIsRunningByMonitor(true);
                    new SocketReceiveModule().execute();
                    break;
                case "RDBMS":
                    new DbReadModule().execute();
                    break;
                default:
                    log.error("Cannot find extractor module class.");
                    return;
            }

            if (isFirstRun)
                isFirstRun = false;
        } catch (Exception e) {
            log.error("An error occurred while thread processing. It will be restarted : {}", e.getMessage());
        }
    }

    private static void monitorForSocket() {
        try {
            if (getIsRunningByMonitor()) {
                final Agent agent = AgentUtil.getAgent(agentType, agentName);
                if (Constant.YN_N.equalsIgnoreCase(agent.getUseYn())) {
                    setIsRunningByMonitor(false);

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
        } catch (Exception e) {
            log.error("An error occurred while thread processing. It will be restarted : {}", e.getMessage());
        }
    }

}