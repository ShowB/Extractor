package com.snet.smore.extractor.module;

import com.google.gson.*;
import com.snet.smore.common.domain.DbInfo;
import com.snet.smore.common.util.DbUtil;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.StringUtil;
import com.snet.smore.common.constant.FileStatusPrefix;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.UUID;

@Slf4j
public class DbReadModule {
    public void execute() {
        Connection conn = null;

        String dbname = EnvManager.getProperty("extractor.source.db.name");
        String url = EnvManager.getProperty("extractor.source.db.url");
        String tablename = EnvManager.getProperty("extractor.source.db.table.name");
        String pkname = EnvManager.getProperty("extractor.source.db.pk.name");
        String sort = EnvManager.getProperty("extractor.source.db.pk.sort");

        if (StringUtil.isBlank(dbname)) {
            log.error("Cannot find value [extractor.source.db.name]. Job will be restarted.");
            return;
        }

        if (StringUtil.isBlank(url)) {
            log.error("Cannot find value [extractor.source.db.url]. Job will be restarted.");
            return;
        }

        if (StringUtil.isBlank(tablename)) {
            log.error("Cannot find value [extractor.source.db.table.name]. Job will be restarted.");
            return;
        }

        if (StringUtil.isBlank(pkname)) {
            log.error("Cannot find value [extractor.source.db.pk.name]. Job will be restarted.");
            return;
        }

        if (StringUtil.isBlank(sort)) {
            log.error("Cannot find value [extractor.source.db.pk.sort]. Job will be restarted.");
            return;
        }

        String[] pknames = pkname.split(",");

        StringBuilder whereClause = new StringBuilder();
        StringBuilder orderByClause = new StringBuilder();
        JsonObject lastPKJson = getPKInfo();

        if (lastPKJson.keySet().size() > 0) {
            String compare;
            if (sort.equalsIgnoreCase("ASC"))
                compare = " > ";
            else
                compare = " < ";

            for (int i = 0; i < pknames.length; i++) {
                String field = pknames[i].trim();

                if (i == 0) {
                    whereClause.append(" WHERE ");
                    orderByClause.append(" ORDER BY ");
                } else {
                    whereClause.append(" AND ");
                    orderByClause.append(", ");
                }

                whereClause.append(field).append(compare).append(lastPKJson.get(field));
                orderByClause.append(field).append(" ").append(sort.toUpperCase());
            }
        }

        log.info("Where Clause: [{}]", whereClause);
        log.info("Order By Clause: [{}]", orderByClause);

        int interval;
        int intervalDefault = 60;

        try {
            interval = Integer.parseInt(EnvManager.getProperty("extractor.source.db.read-interval"));
        } catch (Exception e) {
            log.info("Cannot convert value [extractor.source.db.read-interval]. " +
                    "System will be set default value: {} (seconds)", intervalDefault);
            interval = intervalDefault;
        }


        String countSql;
        String querySql;
        int pageCount = 1000;

        if (dbname.equalsIgnoreCase("mariadb")) {
            countSql = "SELECT COUNT(*) AS COUNT FROM " + tablename + whereClause;
            querySql = "SELECT * FROM " + tablename + whereClause + orderByClause + " LIMIT ?, " + pageCount;
        } else {
            log.error("Cannot convert value [extractor.source.db.name]. Job will be restarted.");
            return;
        }

        DbInfo info = new DbInfo();
        info.setUrl(url);
        info.setUsername(EnvManager.getProperty("extractor.source.db.username"));
        info.setPassword(EnvManager.getProperty("extractor.source.db.password"));

        int totalCnt = 0;
        try {
            conn = DbUtil.getConnection(info);

            PreparedStatement pstmt = conn.prepareStatement(countSql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                totalCnt = rs.getInt("COUNT");
            }
        } catch (SQLException e) {
            log.error("An error occurred while counting target db rows.", e);
        }

        if (totalCnt > 0)
            log.info("Start to read RDBMS Data.. Targets are {} records.", totalCnt);
        else
            log.info("There is no target records. Job will be restarted.");

        JsonObject row = new JsonObject();
        int cnt = 0;

        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        try {
            PreparedStatement pstmt = conn.prepareStatement(querySql);

            for (int i = 0; i < totalCnt; i += pageCount) {
                JsonArray jsonArray = new JsonArray();

                pstmt.setInt(1, i);

                ResultSet rs = pstmt.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {
                    row = new JsonObject();

                    for (int j = 1; j <= columnCount; j++) {
                        String key = metaData.getColumnName(j);
                        String value = rs.getString(j);
                        row.addProperty(key, value);
                    }

                    jsonArray.add(row);

                    cnt++;
                }

                System.out.println(cnt);
                rs.close();
                createFile(gson.toJson(jsonArray));

                JsonObject pkJson = new JsonObject();

                for (String s : pknames) {
                    String field = s.trim();
                    if (row.keySet().contains(field)) {
                        JsonElement value = row.get(field);
                        pkJson.add(field, value);
                        log.info("The Primary Key info was successfully updated. [{}={}]", field, value);
                    }
                }

                storePKFile(pkJson);

            }

            pstmt.close();
            conn.close();

        } catch (SQLException e) {
            log.error("An error occurred while converting to json.", e);
        }

        log.info("DB Read job completed. [completed records: {}] / [target records: {}]", cnt, totalCnt);

        try {
            log.info("Job will sleep for {} seconds ...", interval);
            Thread.sleep(interval * 1000);
        } catch (Exception e) {
            log.error("An error occurred while thread sleeping.", e);
        }

    }

    private void createFile(String str) {
        String root = EnvManager.getProperty("extractor.target.file.dir");
        String fileName = System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        Path newFile = null;
        FileChannel fileChannel = null;

        try {
            Files.createDirectories(Paths.get(root));
            newFile = Files.createFile(Paths.get(root, FileStatusPrefix.TEMP.getPrefix() + fileName));
            fileChannel = FileChannel.open(newFile, StandardOpenOption.WRITE);
            fileChannel.write(ByteBuffer.wrap(str.getBytes()));

            log.info("File created. --> {}", Files.move(newFile, Paths.get(root, fileName)));

        } catch (Exception e) {
            log.error("An error occurred while writing file. {}", newFile);
        } finally {
            try {
                if (fileChannel != null && fileChannel.isOpen())
                    fileChannel.close();
            } catch (IOException e) {
                log.error("An error occurred while writing file. {}", newFile);
            }
        }
    }

    private void storePKFile(JsonObject pkJson) {
        if (pkJson.size() < 1)
            return;

        Path path = Paths.get("config", "last_pk.info");

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            channel.write(ByteBuffer.wrap(pkJson.toString().getBytes()));
        } catch (IOException e) {
            log.error("An error occurred while storing last PK info file.", e);
        }
    }

    private JsonObject getPKInfo() {
        JsonObject json = new JsonObject();
        JsonParser parser = new JsonParser();

        Path path = Paths.get("config", "last_pk.info");

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate((int) Files.size(path));
            channel.read(byteBuffer);
            byteBuffer.flip();

            String s = Charset.defaultCharset().decode(byteBuffer).toString();
            json = parser.parse(s).getAsJsonObject();
        } catch (IOException e) {
            log.info("Cannot find [last_pk.info] file. All records of table will be read.");
        }

        return json;
    }
}
