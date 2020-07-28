package com.snet.smore.extractor.util;

public enum FileStatusPrefix {
    COMPLETE("cmpl_"),
    ERROR("err_"),
    TEMP("tmp_");

    private String prefix;

    FileStatusPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }
}
