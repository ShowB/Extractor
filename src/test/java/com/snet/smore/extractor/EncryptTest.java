package com.snet.smore.extractor;

import com.snet.smore.common.util.CommonUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class EncryptTest {
    @Test
    public void test() {
        String username = "smart";
        String password = "smart123$%^";

        String encUsername = CommonUtil.getEncrypt(username);
        String encPassword = CommonUtil.getEncrypt(password);

        String decUsername = CommonUtil.getDecrypt(encUsername);
        String decPassword = CommonUtil.getDecrypt(encPassword);


        System.out.println("username: \t" + username);
        System.out.println("password: \t" + password);
        System.out.println("encUsername: \t" + encUsername);
        System.out.println("encPassword: \t" + encPassword);
        System.out.println("decUsername: \t" + decUsername);
        System.out.println("decPassword: \t" + decPassword);

        Assert.assertEquals(username, decUsername);
        Assert.assertEquals(password, decPassword);
    }

    @Test
    @Ignore
    public void test2() {
        System.out.println(CommonUtil.getEncrypt("ZTPM_DW"));
    }
}
