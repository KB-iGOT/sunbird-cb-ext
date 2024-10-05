package org.sunbird.consumer.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.sunbird.common.util.CbExtServerProperties;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.Key;

@Component
public class EncryptionService {
    private static Logger log = LoggerFactory.getLogger(EncryptionService.class);

    @Autowired
    CbExtServerProperties serverProperties;


    private static Cipher c;

    static String ALGORITHM = "AES";
    int ITERATIONS = 3;
    static byte[] keyValue =
            new byte[]{'T', 'h', 'i', 's', 'A', 's', 'I', 'S', 'e', 'r', 'c', 'e', 'K', 't', 'e', 'y'};

    static {
        try {
            Key key = generateKey();
            c = Cipher.getInstance(ALGORITHM);
            c.init(Cipher.ENCRYPT_MODE, key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static Key generateKey() {
        return new SecretKeySpec(keyValue, ALGORITHM);
    }

    public String encryptData(String value) {
        String valueToEnc = null;
        String encryption_key = serverProperties.getPublicAssessmentEncryptionKey();
        String eValue = value;
        for (int i = 0; i < ITERATIONS; i++) {
            valueToEnc = encryption_key + eValue;
            byte[] encValue = new byte[0];
            try {
                encValue = c.doFinal(valueToEnc.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                log.error("Exception while encrypting user data, with message : " + e.getMessage(), e);
            }
            eValue = new BASE64Encoder().encode(encValue);
        }
        return eValue;
    }
}
