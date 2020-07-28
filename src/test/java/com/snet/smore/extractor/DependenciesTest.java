package com.snet.smore.extractor;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.extractor.domain.TestDomain;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class DependenciesTest {
    @Test
    public void test() {
        log.debug("상위 디펜던시의 설정 내용을 그대로 가져오는지 테스트 중");
        log.info("상위 디펜던시의 설정 내용을 그대로 가져오는지 테스트 중");
        log.error("상위 디펜던시의 설정 내용을 그대로 가져오는지 테스트 중");

        log.info(EnvManager.getProperty("ABC"));

        TestDomain domain = new TestDomain();
        domain.setName("domainName");
        log.info(domain.getName());
    }
}
