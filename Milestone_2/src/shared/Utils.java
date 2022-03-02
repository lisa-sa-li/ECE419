package shared;

import java.math.BigInteger;

import ecs.HashRing;

public class Utils {
    HashRing hashRing;

    public Utils() {
        hashRing = new HashRing();
    }

    public boolean isKeyInRange(BigInteger hash, BigInteger endHash, String key) {
        if (hash != null && endHash == null) {
            return true;
        }

        BigInteger keyHash = hashRing.getHash(key);
        int left = hash.compareTo(keyHash);
        int right = endHash.compareTo(keyHash);
        // return (left >= 0 && right < 0);

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
}
