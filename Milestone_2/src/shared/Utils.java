package shared;

import java.math.BigInteger;
import java.security.MessageDigest;

public class Utils {

    public Utils() {
    }

    public boolean isKeyInRange(BigInteger hash, BigInteger endHash, String key) {
        if (hash != null && endHash == null) {
            return true;
        }

        BigInteger keyHash = getHash(key);
        int left = hash.compareTo(keyHash);
        int right = endHash.compareTo(keyHash);

        int isEndHashLarger = endHash.compareTo(hash);
        // a.compareTo(b)
        // 0 = equal
        // 1 = a > b
        // -1 = a < b

        if (isEndHashLarger > 0) {
            // left = 12, right = 20, tohash 18
            return (left <= 0 && right > 0);
        } else {
            // left = 99, right = 2, tohash 1
            return (left <= 0 || right > 0);
        }

        // Three cases where the node is responsible for the key
        // Case 1: inHash <= key < endHash
        // Case 2: inHash >= endHash and key >= inHash and key >= endHash
        // Case 3: inHash >= endHash and key <= inHash and key <= endHash
        // return ((hash.compareTo(endHash) != 1) && (hash.compareTo(keyHash) != 1) &&
        // (endHash.compareTo(keyHash) != -1))
        // ||
        // ((hash.compareTo(endHash) != -1) && (hash.compareTo(keyHash) != 1) &&
        // (endHash.compareTo(keyHash) != 1))
        // ||
        // ((hash.compareTo(endHash) != -1) && (hash.compareTo(keyHash) != -1)
        // && (endHash.compareTo(keyHash) != -1));

    }

    public BigInteger getHash(String value) {
        try {
            // get message bytes
            byte[] byteVal = value.getBytes("UTF-8");
            // create md5 instance
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            // convert value to md5 hash (returns bytes)
            byte[] mdDigest = md5.digest(byteVal);

            // convert to string
            StringBuilder stringHash = new StringBuilder();
            for (byte b : mdDigest) {
                // code below: modified code from
                // https://stackoverflow.com/questions/11380062/what-does-value-0xff-do-in-java
                stringHash.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
            }
            return new BigInteger(stringHash.toString(), 16);

        } catch (Exception e) {
            return new BigInteger("00000000000000000000000000000000");
        }
    }

}
