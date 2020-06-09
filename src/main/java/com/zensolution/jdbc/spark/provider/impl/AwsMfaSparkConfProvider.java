package com.zensolution.jdbc.spark.provider.impl;

import com.zensolution.jdbc.spark.internal.ConnectionInfo;
import com.zensolution.jdbc.spark.provider.SparkConfProvider;
import org.ini4j.Ini;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/*
 * This is
 */
public class AwsMfaSparkConfProvider implements SparkConfProvider {

    private static final String AWS_CREDENTIAL_PATH = "aws.credential.path";
    private static final String AWS_PROFILE = "aws.profile";

    @Override
    public Map<String, String> getSparkConf(ConnectionInfo connectionInfo) throws SQLException {
        File awsCredentialFile = getAwsCredentialPath(connectionInfo);
        String awsProfile = getAwsProfile(connectionInfo);

        Map<String, String> prop = new HashMap<>();
        try {
            Ini ini = new Ini(new FileReader(awsCredentialFile));
            prop.put("spark.hadoop.fs.s3a.access.key", ini.get(awsProfile, "aws_access_key_id"));
            prop.put("spark.hadoop.fs.s3a.secret.key", ini.get(awsProfile, "aws_secret_access_key"));
            prop.put("spark.hadoop.fs.s3a.session.token", ini.get(awsProfile, "aws_session_token"));
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return prop;
    }

    private File getAwsCredentialPath(ConnectionInfo connectionInfo) throws SQLException {
        String awsCredential = connectionInfo.getProperties().getProperty(AWS_CREDENTIAL_PATH);
        if ( awsCredential==null || awsCredential.trim().isEmpty()) {
            throw new SQLException(String.format("Please provide driver property '%s'", AWS_CREDENTIAL_PATH));
        }
        File awsCredentialFile = new File(awsCredential);
        if ( !awsCredentialFile.exists() || !awsCredentialFile.isFile() ) {
            throw new SQLException(String.format("Please provide a valid aws credential file at driver property '%s'", AWS_CREDENTIAL_PATH));
        }
        return awsCredentialFile;
    }

    private String getAwsProfile(ConnectionInfo connectionInfo) throws SQLException {
        String awsProfile = connectionInfo.getProperties().getProperty(AWS_PROFILE);
        if ( awsProfile==null || awsProfile.trim().isEmpty()) {
            throw new SQLException(String.format("Please provide driver property '%s'", AWS_PROFILE));
        }
        return awsProfile;
    }
}
