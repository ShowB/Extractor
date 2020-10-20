package com.snet.smore.extractor;

import com.snet.smore.common.util.EncryptUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class EncryptTest {
    @Test
    public void test() {
        String username = "smart";
        String password = "smart123$%^";

        String encUsername = EncryptUtil.getEncrypt(username);
        String encPassword = EncryptUtil.getEncrypt(password);

        String decUsername = EncryptUtil.getDecrypt(encUsername);
        String decPassword = EncryptUtil.getDecrypt(encPassword);


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
        System.out.println(EncryptUtil.getEncrypt("ZTPM_DW"));
        System.out.println(EncryptUtil.getEncrypt("xvmyhzsi"));
        System.out.println(EncryptUtil.getEncrypt("7E1lv5WSojNQwSrksqc4iTecDqC4SDjG"));
//        factory.setUsername("xvmyhzsi");
//        factory.setPassword("7E1lv5WSojNQwSrksqc4iTecDqC4SDjG");
    }
}
