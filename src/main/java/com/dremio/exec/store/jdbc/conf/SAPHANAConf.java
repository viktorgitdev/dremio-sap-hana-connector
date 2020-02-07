/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.dremio.exec.store.jdbc.conf;

 import static com.google.common.base.Preconditions.checkNotNull;

 import java.io.IOException;
 import java.net.URI;
 import java.sql.SQLException;
 import java.util.Properties;


 import java.util.Properties;
 import org.apache.log4j.Logger;
 import javax.validation.constraints.Max;
 import javax.validation.constraints.Min;
 import javax.validation.constraints.NotBlank;
 import com.dremio.exec.catalog.conf.DisplayMetadata;
 import com.dremio.exec.catalog.conf.NotMetadataImpacting;
 import com.dremio.exec.catalog.conf.Secret;
 import com.dremio.exec.catalog.conf.SourceType;
 import com.dremio.exec.server.SabotContext;
 import com.dremio.exec.store.jdbc.CloseableDataSource;
 import com.dremio.exec.store.jdbc.DataSources;
 import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
 import com.dremio.exec.store.jdbc.JdbcStoragePlugin.Config;
 import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
 import com.google.common.annotations.VisibleForTesting;
 import com.google.common.base.Strings;
 import io.protostuff.Tag;

/**
 * Configuration for SAPHANAConf sources.
 */
@SourceType(value = "SAP-HANA-2", label = "SAP-HANA-2")
public class SAPHANAConf extends AbstractArpConf<SAPHANAConf> {

  private static final String ARP_FILENAME = "arp/implementation/sap-hana-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "com.sap.db.jdbc.Driver";
  private static Logger logger = Logger.getLogger(SAPHANAConf.class);
  
  

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "Host")
  public String host;
  
  @NotBlank
  @Tag(2)
  @Min(1)
  @Max(65535)
  @DisplayMetadata(label = "Port")
  public String port;

  @NotBlank
  @Tag(3)
  @DisplayMetadata(label = "Username")
  public String username;

  @NotBlank
  @Tag(4)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password;

  @Tag(5)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 200;

  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String host = checkNotNull(this.host, "Missing host.");
    final String username = checkNotNull(this.username, "Missing username.");
    final String password = checkNotNull(this.password, "Missing password.");
    final String port = checkNotNull(this.port, "Missing port.");


    final String connect = String.format("jdbc:sap://%s:%s", host, port);
    logger.info("url to SAP HANA: " + connect);
    return connect;
  }

  @Override
  @VisibleForTesting
  public Config toPluginConfig(SabotContext context) {
    logger.info("Connecting to SAP HANA");
    logger.info("ARP_DIALECT: " + ARP_DIALECT);
    logger.info("DRIVER: " + DRIVER);
    
    logger.info("H:" + host);
    logger.info("P:" + port);
    logger.info("U:" + username);
    logger.info("PWD:" + password);
    logger.info("F:" + fetchSize);
    


  return JdbcStoragePlugin.Config.newBuilder()
        .withDialect(getDialect())
        .withFetchSize(fetchSize)
        .withDatasourceFactory(this::newDataSource)
        .clearHiddenSchemas()
        .addHiddenSchema("SYSTEM")
        .build();
  }

  private CloseableDataSource newDataSource() {
    logger.info("NewDataSource to SAP HANA");
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
      toJdbcConnectionString(), username, password, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
  }

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}
